# inventory_service.py
# pip install fastapi uvicorn prometheus-client pydantic psutil

import os, time, random, asyncio, logging, json, re, datetime
from typing import Dict
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

SERVICE = os.getenv("SERVICE", "inventory")
FAULT = {"mode": os.getenv("FAULT", "none"), "p_error": 0.2, "latency_ms": 300, "cpu_ms": 200}

# ---- Logging (single common file across instances) ----
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger("inventory_common")
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    fh = logging.FileHandler("logs/inventory_common.log")
    fh.setLevel(logging.ERROR)
    fh.setFormatter(logging.Formatter("%(asctime)s %(process)d %(levelname)s %(message)s"))
    logger.addHandler(fh)

# ---- Metrics ----
REQS = Counter("http_requests_total", "Total HTTP requests", ["service", "method", "route", "status"])
LAT = Histogram(
    "http_request_duration_seconds",
    "Request latency seconds",
    ["service", "route", "method"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.0, 5.0)
)
ERRS = Counter("http_errors_total", "5xx error count", ["service", "route", "status"])
INV_SIZE = Gauge("inventory_items", "Current inventory items", ["service"])

# ---- Data model / store ----
class Item(BaseModel):
    name: str = Field(..., min_length=1)
    price: float = Field(..., ge=0.0)
    manufacturer: str = Field(..., min_length=1)

INVENTORY: Dict[int, Item] = {}

# (Removed rigorous simulation helpers to keep endpoints simple)

# ---- App ----
app = FastAPI(title=f"InventoryService[{SERVICE}]")

@app.middleware("http")
async def metrics_mw(request: Request, call_next):
    start = time.perf_counter()
    method = request.method
    resp = None
    try:
        resp = await call_next(request)
        return resp
    except Exception:
        # Log unexpected exceptions
        logger.exception("REQ EXC: %s %s", method, request.url.path)
        raise
    finally:
        # <-- get templated path AFTER call_next()
        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)  # falls back if unknown
        status = str(resp.status_code if resp else 500)
        dur = time.perf_counter() - start

        LAT.labels(SERVICE, route_path, method).observe(dur)
        REQS.labels(SERVICE, method, route_path, status).inc()
        if status.startswith("5"):
            ERRS.labels(SERVICE, route_path, status).inc()
        # Write structured request log line
        try:
            logger.info("REQ %s %s status=%s dur_ms=%.1f", method, route_path, status, dur * 1000.0)
        except Exception:
            pass


@app.get("/healthz")
def healthz():
    response = {"ok": True, "service": SERVICE, "fault": FAULT}
    print(f"HEALTHZ - Response: {response}")
    return response

@app.get("/metrics")
def metrics():
    INV_SIZE.labels(SERVICE).set(len(INVENTORY))
    metrics_data = generate_latest()
    print(f"METRICS - Returning {len(metrics_data)} bytes of metrics data")
    return Response(metrics_data, media_type=CONTENT_TYPE_LATEST)

@app.get("/logs/tail")
def tail_logs(seconds: int = 15, limit: int = 500):
    """Return recent log entries from the common log within the last N seconds.
    Query params: seconds (default 15), limit (default 500).
    """
    log_path = os.path.join("logs", "inventory_common.log")
    if not os.path.exists(log_path):
        return {"entries": [], "count": 0}

    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(seconds=max(0, int(seconds)))
    ts_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3,6}) (\d+) (\w+) (.*)$")

    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.read().splitlines()
    except Exception:
        return {"entries": [], "count": 0}

    parsed = []
    current = None
    for line in lines:
        m = ts_re.match(line)
        if m:
            # finalize previous
            if current is not None:
                parsed.append(current)
            ts_str, pid_str, level, msg = m.groups()
            try:
                ts = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
            except Exception:
                ts = None
            current = {
                "ts": ts_str,
                "pid": int(pid_str) if pid_str.isdigit() else None,
                "level": level,
                "message": msg,
                "_tsobj": ts,
            }
        else:
            if current is not None:
                # append continuation (e.g., traceback lines)
                current["message"] += "\n" + line
            # else skip stray lines
    if current is not None:
        parsed.append(current)

    # filter by cutoff and take the last `limit` entries
    filtered = []
    for e in parsed:
        tsobj = e.get("_tsobj")
        if tsobj is None or tsobj >= cutoff:
            filtered.append({k: v for k, v in e.items() if k != "_tsobj"})

    # keep last N
    entries = filtered[-max(1, int(limit)) :]
    return {"entries": entries, "count": len(entries)}

@app.get("/items/{item_id}")
def get_item(item_id: int):
    print(f"GET_ITEM - Request: item_id={item_id}")
    logger.info("GET /items/%s", item_id)
    item = INVENTORY.get(item_id)
    if not item:
        print(f"GET_ITEM - MISS id={item_id}")
        raise HTTPException(404, "item not found")

    response = {"id": item_id, **item.model_dump()}
    print(f"GET_ITEM - Response: {response}")
    logger.info("GET /items/%s -> 200", item_id)
    return response

@app.put("/items/{item_id}")
def put_item(item_id: int, item: Item):
    print(f"PUT_ITEM - Request: item_id={item_id}, item={item.model_dump()}")
    logger.info("PUT /items/%s body=%s", item_id, json.dumps(item.model_dump(), separators=(",", ":")))
    INVENTORY[item_id] = item
    response = {"ok": True, "id": item_id, **item.model_dump(), "inventory_size": len(INVENTORY)}
    print(f"PUT_ITEM - Response: {response}")
    logger.info("PUT /items/%s -> 200", item_id)
    return response

@app.get("/random-endpoint")
def random_endpoint():
    """Random endpoint that throws null pointer exception for testing"""
    print(f"RANDOM_ENDPOINT - Request received")
    logger.info("GET /random-endpoint")
    
    # Simulate some processing
    time.sleep(0.1)
    
    # Simulate a null pointer exception scenario
    try:
        # This will cause a null pointer exception
        data = None
        result = data.get("some_key")  # This will raise AttributeError (similar to null pointer)
        return {"result": result}
    except AttributeError as e:
        print(f"RANDOM_ENDPOINT - Error: {e}")
        logger.exception("GET /random-endpoint -> 500 NPE")
        # Re-raise as a 500 error to simulate server error
        raise HTTPException(status_code=500, detail=f"Null pointer exception: {str(e)}")

@app.get("/random-endpoint/{trigger}")
def random_endpoint_with_trigger(trigger: str):
    """Random endpoint that can be triggered to cause errors"""
    print(f"RANDOM_ENDPOINT - Request with trigger: {trigger}")
    logger.info("GET /random-endpoint/%s", trigger)
    
    # Only cause error if trigger is "error"
    if trigger == "error":
        try:
            # Simulate null pointer exception
            data = None
            result = data.get("some_key")
            return {"result": result}
        except AttributeError as e:
            print(f"RANDOM_ENDPOINT - Triggered error: {e}")
            logger.exception("GET /random-endpoint/%s -> 500 NPE", trigger)
            raise HTTPException(status_code=500, detail=f"Triggered null pointer exception: {str(e)}")
    else:
        logger.info("GET /random-endpoint/%s -> 200 ok", trigger)
        return {"status": "ok", "trigger": trigger, "message": "No error triggered"}

# ---- Fault control (runtime) ----
@app.post("/faults")
async def set_faults(payload: dict):
    """
    Set fault mode:
      {"mode":"latency","latency_ms":500}
      {"mode":"errors","p_error":0.4}
      {"mode":"cpu","cpu_ms":300}
      {"mode":"oom_safe"} or {"mode":"crash"}
      {"mode":"none"}
    """
    print(f"SET_FAULTS - Request: {payload}")
    FAULT.update(payload or {})
    response = {"ok": True, "fault": FAULT}
    print(f"SET_FAULTS - Response: {response}")
    return response

async def maybe_fault(route_path: str):
    mode = FAULT.get("mode", "none")
    if mode == "none":
        return
    if mode == "latency":
        ms = float(FAULT.get("latency_ms", 300))
        jitter = ms * 0.5
        delay = max(0.0, (ms + random.uniform(-jitter, jitter)) / 1000.0)
        await asyncio.sleep(delay)
    elif mode == "errors":
        p = float(FAULT.get("p_error", 0.2))
        if random.random() < p:
            logger.error("Injected 500 error")
            raise HTTPException(500, "injected error")
    elif mode == "cpu":
        ms = int(FAULT.get("cpu_ms", 200))
        end = time.perf_counter() + (ms / 1000.0)
        # busy-wait to burn CPU
        while time.perf_counter() < end:
            pass
    elif mode == "oom_safe":
        # emulate OOM via 500, do NOT crash process (safe for class)
        logger.error("Emulated OOM (safe)")
        raise HTTPException(500, "emulated OOM")
    elif mode == "crash":
        # ⚠️ will terminate the process intentionally
        logger.critical("Crashing process on purpose")
        os._exit(137)
