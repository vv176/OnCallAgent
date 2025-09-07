import os, asyncio
from typing import List
from urllib.parse import urlparse
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import httpx

BACKENDS: List[str] = [
    u.strip() for u in os.getenv("BACKENDS", "http://localhost:8001,http://localhost:8002").split(",")
    if u.strip()
]
if not BACKENDS:
    raise RuntimeError("Set BACKENDS env var, e.g. BACKENDS='http://localhost:8001,http://localhost:8002'")

# ---- timeouts (works with your httpx version)
CONNECT_TIMEOUT = float(os.getenv("LB_CONNECT_TIMEOUT", "3.0"))
READ_TIMEOUT    = float(os.getenv("LB_READ_TIMEOUT", "5.0"))
WRITE_TIMEOUT   = float(os.getenv("LB_WRITE_TIMEOUT", "5.0"))
POOL_TIMEOUT    = float(os.getenv("LB_POOL_TIMEOUT", "3.0"))

app = FastAPI(title="RR Load Balancer", version="0.2.0")

MANAGER_URL = os.getenv("PM_URL", "http://127.0.0.1:7070")
REFRESH_EVERY_SEC = int(os.getenv("LB_REFRESH_SEC", "5"))
# round-robin cursor + lock for concurrent requests
_rr_idx = 0
_rr_lock = asyncio.Lock()

async def pick_primary_and_alt() -> tuple[str, str | None]:
    """Advance RR exactly once; compute alt without advancing."""
    global _rr_idx
    # Use dynamic backends from process manager, fallback to static BACKENDS
    backends = getattr(app.state, 'backends', BACKENDS)
    n = len(backends)
    async with _rr_lock:
        primary = backends[_rr_idx % n]
        # advance the cursor ONCE for next request
        _rr_idx = (_rr_idx + 1) % n
        alt = backends[_rr_idx % n] if n > 1 else None
    return primary, alt

@app.on_event("startup")
async def _startup():
    app.state.client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT, write=WRITE_TIMEOUT, pool=POOL_TIMEOUT),
        follow_redirects=False,
    )
    app.state.backends = BACKENDS[:]  # initial list
    async def refresh():
        while True:
            try:
                r = await app.state.client.get(f"{MANAGER_URL}/backends")
                if r.status_code == 200:
                    app.state.backends = r.json().get("backends", app.state.backends) or app.state.backends
            except Exception:
                pass
            await asyncio.sleep(REFRESH_EVERY_SEC)
    asyncio.create_task(refresh())

@app.on_event("shutdown")
async def _shutdown():
    await app.state.client.aclose()

@app.get("/healthz")
async def healthz():
    # Show dynamic backends from process manager
    backends = getattr(app.state, 'backends', BACKENDS)
    return {"ok": True, "backends": backends}

@app.api_route("/{path:path}", methods=["GET","POST","PUT","PATCH","DELETE","OPTIONS","HEAD"])
async def proxy(path: str, request: Request):
    """
    - Round-robin pick a backend (advances once per request).
    - Forward method, headers (minus hop-by-hop), query, body.
    - On error, try the alternate backend (computed without advancing).
    """
    # Log incoming request
    method = request.method
    query = f"?{request.url.query}" if request.url.query else ""
    print(f"LB_REQUEST: {method} /{path}{query}")
    print(f"LB_REQUEST_HEADERS: {dict(request.headers)}")
    
    primary, alt = await pick_primary_and_alt()
    print(f"LB_SELECTED_BACKENDS: primary={primary}, alt={alt}")

    hop = {"connection","keep-alive","proxy-authenticate","proxy-authorization","te",
           "trailer","transfer-encoding","upgrade","host"}
    headers = {k: v for k, v in request.headers.items() if k.lower() not in hop}
    body = await request.body()
    
    if body:
        print(f"LB_REQUEST_BODY: {body.decode('utf-8')}")
    
    client: httpx.AsyncClient = app.state.client

    tried = []
    for i, backend in enumerate(filter(None, [primary, alt])):
        tried.append(backend)
        url = f"{backend}/{path}{query}"
        print(f"LB_TRYING_BACKEND_{i+1}: {url}")
        try:
            r = await client.request(method, url, content=body, headers=headers)
            
            # Log successful response
            print(f"LB_RESPONSE: {r.status_code} from {url}")
            print(f"LB_RESPONSE_HEADERS: {dict(r.headers)}")
            if r.content:
                try:
                    response_body = r.content.decode('utf-8')
                    print(f"LB_RESPONSE_BODY: {response_body}")
                except UnicodeDecodeError:
                    print(f"LB_RESPONSE_BODY: <binary data, {len(r.content)} bytes>")
            
            # pass-through headers + stamp which backend handled it
            resp_headers = {k: v for k, v in r.headers.items() if k.lower() not in hop}
            resp_headers["X-LB-Backend"] = urlparse(backend).netloc
            print(f"LB_SUCCESS: Backend {urlparse(backend).netloc} handled request")
            return Response(content=r.content, status_code=r.status_code, headers=resp_headers)
        except httpx.HTTPError as e:
            print(f"LB_BACKEND_ERROR_{i+1}: {url} - {str(e)}")
            continue

    # Log final error
    error_response = {"error": "upstream_unavailable", "tried": tried}
    print(f"LB_FINAL_ERROR: {error_response}")
    return JSONResponse(error_response, status_code=502)
