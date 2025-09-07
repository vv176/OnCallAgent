# traffic_gen.py
# pip install httpx anyio pydantic
"""
Tunable traffic generator for your LB (http://localhost:9000 by default).

Features
- Open-loop RPS pacing (accurate scheduling) with a concurrency cap.
- Load profiles: constant, spike, ramp, sine.
- Request mix: GET vs PUT (configurable).
- Payload size control (to stress CPU/JSON parsing).
- Rolling & final stats (p50/p95/p99, error rate).
- Hot key traffic (zipf/hotspot) or uniform item id distribution.

Examples
# 1) Constant 50 rps for 2 minutes (70% GET / 30% PUT), moderate payload
python traffic_gen.py --url http://localhost:9000 --rps 50 --duration 120 --put-frac 0.3 --payload-kb 2

# 2) Spike load: base 30 rps, spike 4x at t=60s
python traffic_gen.py --rps 30 --duration 120 --profile spike --spike-at 60 --spike-mult 4

# 3) Ramp-spike: mild 10 rps for 1 min, then high 100 rps for 5 mins
python traffic_gen.py --duration 360 --profile ramp-spike --mild-rps 10 --high-rps 100 --mild-duration 60

# 3) Ramp from 10 -> 150 rps across 3 minutes
python traffic_gen.py --profile ramp --ramp-from 10 --ramp-to 150 --duration 180

# 4) Sine wave around 50 rps ±30 (period 90s)
python traffic_gen.py --profile sine --rps 50 --sine-amplitude 30 --sine-period 90 --duration 300

# 5) Hotspot IDs (90% traffic hits 10% of keys)
python traffic_gen.py --hot-frac 0.9 --hotset 100 --keyspace 2000
"""
from __future__ import annotations
import argparse, asyncio, math, os, random, string, time
from dataclasses import dataclass
from typing import Deque, Optional, Tuple
import anyio
import httpx
from collections import deque

# ----------------------------- CLI -----------------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Tunable LB traffic generator")
    ap.add_argument("--url", default=os.getenv("LB_URL", "http://localhost:9000"),
                    help="Load balancer base URL (default: http://localhost:9000)")
    ap.add_argument("--duration", type=float, default=120.0, help="Test duration seconds")
    ap.add_argument("--rps", type=float, default=30.0, help="Base requests per second")
    ap.add_argument("--max-concurrency", type=int, default=200, help="Max in-flight requests")
    ap.add_argument("--put-frac", type=float, default=0.3, help="Fraction of PUT requests [0..1]")
    ap.add_argument("--payload-kb", type=float, default=1.0, help="Approx payload size per PUT (KB)")
    ap.add_argument("--report-every", type=float, default=5.0, help="Seconds between rolling stats print")

    # Profiles
    ap.add_argument("--profile", choices=["constant", "spike", "ramp", "sine", "ramp-spike", "agentic"], default="constant")
    ap.add_argument("--spike-at", type=float, default=60.0, help="Spike time (s) for 'spike' profile")
    ap.add_argument("--spike-mult", type=float, default=3.0, help="Spike multiple for 'spike' profile")
    ap.add_argument("--ramp-from", type=float, default=10.0, help="Start rps for 'ramp' profile")
    ap.add_argument("--ramp-to", type=float, default=150.0, help="End rps for 'ramp' profile")
    ap.add_argument("--sine-amplitude", type=float, default=20.0, help="Amplitude for 'sine' profile")
    ap.add_argument("--sine-period", type=float, default=90.0, help="Period (s) for 'sine' profile")
    
    # Ramp-spike profile parameters
    ap.add_argument("--mild-rps", type=float, default=10.0, help="Mild traffic RPS for 'ramp-spike' profile")
    ap.add_argument("--high-rps", type=float, default=100.0, help="High traffic RPS for 'ramp-spike' profile")
    ap.add_argument("--mild-duration", type=float, default=60.0, help="Duration of mild traffic (s) for 'ramp-spike' profile")
    
    # Agentic profile parameters
    ap.add_argument("--normal-rps", type=float, default=15.0, help="Normal traffic RPS for 'agentic' profile")
    ap.add_argument("--error-rps", type=float, default=10.0, help="Error traffic RPS for 'agentic' profile")
    ap.add_argument("--normal-duration", type=float, default=60.0, help="Duration of normal traffic (s) for 'agentic' profile")
    ap.add_argument("--error-duration", type=float, default=60.0, help="Duration of error traffic (s) for 'agentic' profile")

    # Keys distribution
    ap.add_argument("--keyspace", type=int, default=5000, help="Distinct item IDs")
    ap.add_argument("--hot-frac", type=float, default=0.0, help="Fraction of traffic to hot set [0..1]")
    ap.add_argument("--hotset", type=int, default=100, help="Size of hot set (used if hot-frac>0)")

    # Misc
    ap.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout seconds")
    ap.add_argument("--seed", type=int, default=None, help="RNG seed (for repeatability)")
    return ap.parse_args()

# --------------------------- Utilities --------------------------
def quantiles(values, qs):
    if not values:
        return [float("nan") for _ in qs]
    xs = sorted(values)
    n = len(xs)
    out = []
    for q in qs:
        if n == 1:
            out.append(xs[0])
            continue
        pos = (n - 1) * q
        i = int(math.floor(pos))
        j = min(i + 1, n - 1)
        frac = pos - i
        out.append(xs[i] * (1 - frac) + xs[j] * frac)
    return out

def kb_string(kb: float) -> str:
    n = max(0, int(kb * 1024))
    # produce compact ASCII to avoid too much JSON overhead per char
    return ("x" * n) if n <= 8192 else ("x" * 8192) + ("y" * (n - 8192))

@dataclass
class Stats:
    latencies_ms: Deque[float]
    start_ts: float
    total: int = 0
    ok: int = 0
    errors: int = 0

    def snapshot(self) -> Tuple[int, int, int, float, float, float, float]:
        arr = list(self.latencies_ms)
        p50, p95, p99 = quantiles(arr, [0.50, 0.95, 0.99])
        now = time.perf_counter()
        elapsed = max(1e-6, now - self.start_ts)
        rps_obs = self.total / elapsed
        err_pct = (self.errors / max(1, self.total)) * 100.0
        return self.total, self.ok, self.errors, rps_obs, p50, p95, p99, err_pct

# --------------------------- Generator --------------------------
class TrafficGen:
    def __init__(self, args):
        self.base = args.url.rstrip("/")
        self.duration = args.duration
        self.base_rps = args.rps
        self.profile = args.profile
        self.spike_at = args.spike_at
        self.spike_mult = args.spike_mult
        self.ramp_from = args.ramp_from
        self.ramp_to = args.ramp_to
        self.sine_amp = args.sine_amplitude
        self.sine_period = args.sine_period
        self.mild_rps = args.mild_rps
        self.high_rps = args.high_rps
        self.mild_duration = args.mild_duration
        self.normal_rps = args.normal_rps
        self.error_rps = args.error_rps
        self.normal_duration = args.normal_duration
        self.error_duration = args.error_duration
        self.put_frac = max(0.0, min(1.0, args.put_frac))
        self.payload_kb = max(0.0, args.payload_kb)
        self.keyspace = max(1, args.keyspace)
        self.hot_frac = max(0.0, min(1.0, args.hot_frac))
        self.hotset = max(1, args.hotset)
        self.timeout = args.timeout
        self.max_conc = args.max_concurrency
        self.report_every = args.report_every

        self.stats = Stats(latencies_ms=deque(maxlen=50000), start_ts=time.perf_counter())
        self.sem = asyncio.Semaphore(self.max_conc)
        self.client = httpx.AsyncClient(timeout=self.timeout)
        self.payload_cache = kb_string(self.payload_kb)

        if args.seed is not None:
            random.seed(args.seed)

    def current_rps(self, t: float) -> float:
        if self.profile == "constant":
            return self.base_rps
        if self.profile == "spike":
            return self.base_rps * (self.spike_mult if t >= self.spike_at else 1.0)
        if self.profile == "ramp":
            f = max(0.0, min(1.0, t / max(1e-6, self.duration)))
            return self.ramp_from + (self.ramp_to - self.ramp_from) * f
        if self.profile == "sine":
            # rps(t) = base + A * sin(2π t / period)
            return max(0.0, self.base_rps + self.sine_amp * math.sin(2 * math.pi * t / max(1e-6, self.sine_period)))
        if self.profile == "ramp-spike":
            # Mild traffic for first mild_duration seconds, then high traffic
            if t < self.mild_duration:
                return self.mild_rps
            else:
                return self.high_rps
        if self.profile == "agentic":
            # Normal traffic for first normal_duration seconds, then error traffic
            if t < self.normal_duration:
                return self.normal_rps
            else:
                return self.error_rps
        return self.base_rps

    def pick_id(self) -> int:
        if self.hot_frac <= 0:
            return random.randint(1, self.keyspace)
        # With prob hot_frac, choose from hot set; else from cold space
        if random.random() < self.hot_frac:
            return random.randint(1, min(self.hotset, self.keyspace))
        return random.randint(self.hotset + 1, self.keyspace)

    async def one_request(self, t: float = 0.0):
        id_ = self.pick_id()
        method_put = (random.random() < self.put_frac)

        # For agentic profile, determine if we should call error endpoint
        if self.profile == "agentic" and t >= self.normal_duration:
            # Error phase: 80% error endpoint, 20% normal requests
            if random.random() < 0.8:
                error_path = os.getenv("ERROR_ENDPOINT_PATH", "/random-endpoint/error")
                url = f"{self.base}{error_path}"
                method_put = False  # Buggy endpoint is always GET
            else:
                url = f"{self.base}/items/{id_}"
        else:
            # Normal phase or other profiles: use normal endpoints
            url = f"{self.base}/items/{id_}"

        t0 = time.perf_counter()
        status = 0
        try:
            async with self.sem:
                if method_put:
                    body = {"name": f"item{id_}", "price": 19.5, "manufacturer": "acme", "blob": self.payload_cache}
                    r = await self.client.put(url, json=body)
                else:
                    r = await self.client.get(url)
                status = r.status_code
        except Exception:
            status = 0  # treat as network error
        finally:
            t1 = time.perf_counter()
            self.stats.total += 1
            if 200 <= status < 500:
                self.stats.ok += 1
            else:
                self.stats.errors += 1
            self.stats.latencies_ms.append((t1 - t0) * 1000.0)

    async def reporter(self):
        last_print = time.perf_counter()
        while True:
            await asyncio.sleep(0.5)
            now = time.perf_counter()
            if now - last_print >= self.report_every:
                total, ok, err, rps, p50, p95, p99, err_pct = self.stats.snapshot()
                elapsed = now - self.stats.start_ts
                
                # Add phase indicator for agentic profile
                phase_indicator = ""
                if self.profile == "agentic":
                    if elapsed < self.normal_duration:
                        phase_indicator = " [NORMAL PHASE]"
                    else:
                        phase_indicator = " [ERROR PHASE]"
                
                print(f"[{elapsed:6.1f}s]{phase_indicator} sent={total} ok={ok} err={err} "
                      f"rps~{rps:6.1f} p50={p50:6.1f}ms p95={p95:6.1f}ms p99={p99:6.1f}ms err%={err_pct:4.1f}")
                last_print = now

    async def run(self):
        if self.profile == "agentic":
            print(f"🎯 AGENTIC TRAFFIC GENERATOR")
            print(f"   Target: {self.base}")
            print(f"   Normal Phase: {self.normal_duration}s @ {self.normal_rps} RPS")
            print(f"   Error Phase: {self.error_duration}s @ {self.error_rps} RPS")
            print(f"   Total Duration: {self.duration}s")
            print()
        else:
            print(f"Target: {self.base} | profile={self.profile} | duration={self.duration}s | base_rps={self.base_rps} | "
                  f"put_frac={self.put_frac} | payload_kb={self.payload_kb} | max_conc={self.max_conc}")
        start = time.perf_counter()
        reporter_task = asyncio.create_task(self.reporter())

        # Open-loop scheduler: send requests at scheduled instants based on instantaneous rps(t)
        # We step in small quanta (e.g., 0.2s) and schedule N = rps(t) * dt requests each slice.
        dt = 0.2
        try:
            while True:
                t = time.perf_counter() - start
                if t >= self.duration:
                    break
                rps = self.current_rps(t)
                n = max(0, int(round(rps * dt)))
                # Backpressure: if concurrency is saturated, we'll naturally slow because acquire() waits
                batch = [asyncio.create_task(self.one_request(t)) for _ in range(n)]
                if batch:
                    # don't await here; let them progress while we sleep dt
                    pass
                await asyncio.sleep(dt)
        except KeyboardInterrupt:
            print("\nInterrupted by user.")
        finally:
            await asyncio.sleep(0.5)  # allow in-flight to finish a bit
            reporter_task.cancel()
            with contextlib.suppress(Exception):
                await reporter_task
            await self.client.aclose()

        total, ok, err, rps, p50, p95, p99, err_pct = self.stats.snapshot()
        print("\n=== FINAL ===")
        print(f"sent={total} ok={ok} err={err} rps~{rps:6.1f} "
              f"p50={p50:6.1f}ms p95={p95:6.1f}ms p99={p99:6.1f}ms err%={err_pct:4.1f}")

# --------------------------- main -------------------------------
import contextlib
async def amain():
    args = parse_args()
    gen = TrafficGen(args)
    await gen.run()

if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except RuntimeError:
        # On some event loops, nested run; fallback
        anyio.run(amain)
