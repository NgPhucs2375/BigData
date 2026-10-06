"""Small dependency-free dashboard for the local streaming demo."""
import json
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen

PORT = 8090
SPARK_MASTER = "http://localhost:8080/json/"

PAGE = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MUA Stream Monitor</title><style>
:root{font-family:Inter,system-ui,sans-serif;color:#e8edf3;background:#0b1117;--line:#263340;--muted:#8fa1b3;--cyan:#55d6be;--amber:#f3b562}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 80% -10%,#163b46 0,#0b1117 45%);min-height:100vh}main{max-width:1180px;margin:auto;padding:36px 24px 60px}.eyebrow{color:var(--cyan);font:700 12px ui-monospace,monospace;letter-spacing:.16em;text-transform:uppercase}.head{display:flex;justify-content:space-between;gap:20px;align-items:end;border-bottom:1px solid var(--line);padding-bottom:25px}.head h1{font-size:clamp(32px,6vw,68px);line-height:.95;margin:10px 0 0;letter-spacing:-.06em}.live{border:1px solid #2b665f;color:var(--cyan);padding:8px 11px;border-radius:999px;font:700 12px ui-monospace,monospace}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:24px 0}.card,.panel{border:1px solid var(--line);background:#101922e8;border-radius:12px}.card{padding:18px;min-height:112px}.label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em}.value{font-size:30px;font-weight:700;margin-top:14px}.sub{font-size:12px;color:var(--muted);margin-top:6px}.layout{display:grid;grid-template-columns:1.35fr .65fr;gap:14px}.panel{padding:20px}.panel h2{font-size:16px;margin:0 0 18px}.flow{display:grid;grid-template-columns:1fr 20px 1fr 20px 1fr 20px 1fr;gap:8px;align-items:center}.node{padding:15px 10px;border:1px solid #2e4855;background:#12232a;border-radius:9px;min-height:91px}.node b{display:block;color:var(--cyan);font-size:13px}.node span{display:block;color:var(--muted);font-size:11px;margin-top:9px;line-height:1.35}.arrow{text-align:center;color:var(--amber);font-size:20px}.kv{display:grid;grid-template-columns:110px 1fr;gap:11px;font-size:13px}.kv b{color:var(--muted);font-weight:500}.path{font:11px ui-monospace,monospace;color:#c9d4dd;overflow-wrap:anywhere}.links{display:flex;gap:9px;flex-wrap:wrap;margin-top:18px}.links a{color:var(--cyan);border:1px solid #28554f;border-radius:7px;padding:8px 10px;text-decoration:none;font-size:12px}.footer{color:var(--muted);font-size:12px;margin-top:18px}@media(max-width:800px){.grid{grid-template-columns:repeat(2,1fr)}.layout{grid-template-columns:1fr}.flow{grid-template-columns:1fr}.arrow{transform:rotate(90deg)}}
</style></head><body><main><div class="head"><div><div class="eyebrow">MUA / Big Data control room</div><h1>Stream monitor</h1></div><div class="live" id="live">CONNECTING</div></div><section class="grid"><div class="card"><div class="label">Query state</div><div class="value" id="state">--</div><div class="sub" id="stateSub">waiting for Spark</div></div><div class="card"><div class="label">Workers</div><div class="value" id="workers">--</div><div class="sub">Spark standalone cluster</div></div><div class="card"><div class="label">Memory / worker</div><div class="value" id="memory">--</div><div class="sub">low-memory profile</div></div><div class="card"><div class="label">Trigger</div><div class="value">10s</div><div class="sub">max 1 file / trigger</div></div></section><section class="layout"><div class="panel"><h2>Live data flow</h2><div class="flow"><div class="node"><b>01 / LANDING</b><span>JSONL files<br>HDFS incoming</span></div><div class="arrow">→</div><div class="node"><b>02 / STREAM</b><span>schema + watermark<br>deduplicate</span></div><div class="arrow">→</div><div class="node"><b>03 / CURATED</b><span>Parquet partitions<br>event_date / type</span></div><div class="arrow">→</div><div class="node"><b>04 / BATCH</b><span>ALS / analytics<br>next scheduled run</span></div></div><div class="footer">Last refresh: <span id="updated">--</span></div></div><div class="panel"><h2>Runtime</h2><div class="kv"><b>App</b><span id="app">--</span><b>Source</b><span class="path">/mua/stream/incoming/interactions</span><b>Output</b><span class="path">/mua/stream/curated/interactions</span><b>Checkpoint</b><span class="path">/mua/stream/checkpoints/interactions</span><b>Policy</b><span>watermark 10m / dedup ID</span></div><div class="links"><a href="http://localhost:8080" target="_blank">Spark Master UI ↗</a><a href="http://localhost:9870" target="_blank">HDFS UI ↗</a></div></div></section><div class="footer">Lightweight observability layer for the demo. Spark UI remains the runtime source of truth.</div></main><script>
async function refresh(){try{const r=await fetch('/api/status');const d=await r.json(),a=d.app;document.querySelector('#live').textContent=d.connected?'LIVE':'OFFLINE';document.querySelector('#state').textContent=a?'RUNNING':'IDLE';document.querySelector('#stateSub').textContent=a?(a.duration||'active'):'start stream_interactions.py';document.querySelector('#workers').textContent=d.workers+' / '+d.cores;document.querySelector('#memory').textContent=d.memory+' MB';document.querySelector('#app').textContent=a?a.name:'none';document.querySelector('#updated').textContent=new Date().toLocaleTimeString()}catch(e){document.querySelector('#live').textContent='OFFLINE'}}refresh();setInterval(refresh,5000);
</script></body></html>'''

def status():
    try:
        with urlopen(SPARK_MASTER, timeout=2) as response:
            data = json.load(response)
        app = next((item for item in data.get("activeapps", []) if "stream" in item.get("name", "").lower()), None)
        progress = {}
        try:
            raw = subprocess.check_output(["docker", "exec", "spark-master", "cat", "/tmp/mua-stream-progress.json"], text=True, timeout=2)
            progress = json.loads(raw)
            operators = progress.get("stateOperators", [])
            progress["stateRows"] = operators[0].get("numRowsTotal", 0) if operators else 0
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            pass
        return {"connected": True, "app": app, "workers": data.get("aliveworkers", 0), "cores": data.get("cores", 0), "memory": data.get("memory", 0), "progress": progress}
    except Exception as exc:
        return {"connected": False, "error": str(exc), "app": None, "workers": 0, "cores": 0, "memory": 0, "progress": {}}

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        payload = json.dumps(status()).encode() if self.path == "/api/status" else PAGE.encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json" if self.path == "/api/status" else "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
    def log_message(self, *_args):
        return

print(f"Stream dashboard: http://localhost:{PORT}")
ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
