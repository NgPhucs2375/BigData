"""Small dependency-free dashboard for the local streaming demo."""
import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

PORT = 8090
SPARK_MASTER = "http://localhost:8080/json/"
CODE_DIR = Path(__file__).resolve().parents[1]
LAST_RESULT = CODE_DIR / "data" / "live_train_last.json"
APPLY_TOOL = CODE_DIR / "tools" / "apply_suggestions.py"
TRAIN_CMD = [
    "docker", "exec", "spark-master", "/spark/bin/spark-submit",
    "--master", "spark://spark-master:7077",
    # Worker chỉ có 768MB — executor mặc định 1024MB sẽ kẹt app ở WAITING vĩnh viễn.
    "--executor-memory", "512m", "--driver-memory", "512m",
    "/jobs/buoi08_live_train.py",
]
MAX_LOG_LINES = 400
# Watchdog: job không in dòng nào trong STALL_SECONDS -> coi như treo (Docker/Spark chết)
# và terminate để state về error thay vì kẹt "running" vĩnh viễn.
STALL_SECONDS = 180
MAX_TRAIN_SECONDS = 1200

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

# ---- Live train lab: chạy spark-submit buoi08_live_train.py nền + parse dòng LIVE| ----
_train = {
    "state": "idle",       # idle | running | done | error
    "started": None,       # epoch giây bắt đầu
    "duration": None,      # giây khi kết thúc
    "lines": [],           # log thô tail (phần hiển thị khi lỗi)
    "live": [],            # các event {kind: stage|metric} theo thứ tự
    "metrics": {},         # payload {kind: done}
    "executors": [],       # payload {kind: executors}
    "case": [],           # payload {kind: case_study}
    "error": None,
    "last_output": None,  # epoch giây nhận output cuối cùng (watchdog)
    "rank": None,         # rank ALS của lần chạy hiện tại (argv buoi08)
    "serving_users": None,# số user trong suggestions_top10_live
}
_train_lock = threading.Lock()


def _save_last_result():
    try:
        LAST_RESULT.parent.mkdir(parents=True, exist_ok=True)
        LAST_RESULT.write_text(json.dumps({
            "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "rank": _train["rank"],
            "metrics": _train["metrics"],
            "executors": _train["executors"],
            "case": _train["case"],
        }, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        pass


def _load_last_result():
    try:
        return json.loads(LAST_RESULT.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _train_reader(proc):
    """Đọc stdout của docker exec trong thread riêng.

    encoding utf-8 + errors=replace: trước đây text=True (locale encoding) từng làm
    thread chết vì UnicodeDecodeError trên dòng tiếng Việt -> state kẹt "running".
    """
    try:
        for raw in proc.stdout:
            line = raw.rstrip("\n")
            with _train_lock:
                _train["lines"].append(line)
                del _train["lines"][:-MAX_LOG_LINES]
                _train["last_output"] = time.time()
                marker = "LIVE|" in line
            if not marker:
                continue
            try:
                payload = json.loads(line.split("LIVE|", 1)[1])
            except json.JSONDecodeError:
                continue
            kind = payload.get("kind")
            with _train_lock:
                if kind == "stage" or kind == "metric":
                    _train["live"].append(payload)
                    if kind == "metric" and payload.get("name") == "serving_users":
                        _train["serving_users"] = payload.get("value")
                elif kind == "done":
                    _train["metrics"] = payload.get("metrics", {})
                    _train["serving_users"] = _train["metrics"].get("serving_users")
                elif kind == "executors":
                    _train["executors"] = payload.get("executors", [])
                elif kind == "case_study":
                    _train["case"].append(payload)
    except Exception as exc:  # không được nuốt im lặng
        with _train_lock:
            _train["error"] = f"reader lỗi: {exc}"


def _train_finish(code):
    with _train_lock:
        _train["duration"] = round(time.time() - _train["started"], 1) if _train["started"] else None
        if code == 0 and _train["metrics"]:
            _train["state"] = "done"
            _save_last_result()
        else:
            _train["state"] = "error"
            _train["error"] = _train["error"] or f"spark-submit exit {code}"


def _train_kill(proc, reason):
    with _train_lock:
        _train["error"] = _train["error"] or reason
    try:
        proc.kill()
        proc.wait(timeout=10)
    except (OSError, subprocess.SubprocessError):
        pass


def _train_run(cmd):
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            text=True, encoding="utf-8", errors="replace", bufsize=1,
        )
    except OSError as exc:
        with _train_lock:
            _train["state"] = "error"
            _train["error"] = f"không chạy được docker exec: {exc}"
        return
    with _train_lock:
        _train["last_output"] = time.time()
    threading.Thread(target=_train_reader, args=(proc,), daemon=True).start()
    code = None
    while True:
        try:
            code = proc.wait(timeout=5)
            break
        except subprocess.TimeoutExpired:
            pass
        now = time.time()
        with _train_lock:
            started = _train["started"] or now
            last_output = _train.get("last_output") or started
        idle = now - last_output
        if idle > STALL_SECONDS:
            _train_kill(proc, f"Không nhận output nào trong {int(idle)}s — job treo (Docker/Spark?), đã dừng.")
            code = -1
            break
        if now - started > MAX_TRAIN_SECONDS:
            _train_kill(proc, f"Vượt quá {MAX_TRAIN_SECONDS}s — đã dừng job.")
            code = -1
            break
    _train_finish(code)


def train_start(rank=5):
    try:
        rank = int(rank)
    except (TypeError, ValueError):
        rank = 5
    if rank not in (5, 10, 20):
        return {"ok": False, "error": "rank phải là 5, 10 hoặc 20"}
    with _train_lock:
        if _train["state"] == "running":
            return {"ok": False, "error": "Một job train đang chạy — đợi job hiện tại kết thúc."}
        _train.update(
            state="running", started=time.time(), duration=None,
            lines=[], live=[], metrics={}, executors=[], case=[], error=None,
            last_output=time.time(), rank=rank, serving_users=None,
        )
    threading.Thread(target=_train_run, args=(TRAIN_CMD + [str(rank)],), daemon=True).start()
    return {"ok": True, "rank": rank}


def train_reset():
    """Bỏ job kẹt (vd. Docker chết giữa chừng) mà không cần restart server."""
    with _train_lock:
        was_running = _train["state"] == "running"
        _train.update(
            state="idle", started=None, duration=None, lines=[], live=[],
            metrics={}, executors=[], case=[], error=None, last_output=None,
            rank=None, serving_users=None,
        )
    return {"ok": True, "was_running": was_running}


def train_status():
    cluster = status()
    with _train_lock:
        train_payload = {
            "state": _train["state"],
            "duration": _train["duration"],
            "elapsed": round(time.time() - _train["started"], 1)
            if _train["started"] and _train["state"] == "running" else _train["duration"],
            "live": list(_train["live"]),
            "metrics": dict(_train["metrics"]),
            "executors": list(_train["executors"]),
            "case": list(_train["case"]),
            "log": list(_train["lines"][-40:]),
            "error": _train["error"],
            "rank": _train["rank"],
            "serving_users": _train["serving_users"],
        }
    last = _load_last_result() if train_payload["state"] == "idle" else None
    return {
        "connected": cluster.get("connected", False),
        "workers": cluster.get("workers", 0),
        "cores": cluster.get("cores", 0),
        "memory": cluster.get("memory", 0),
        "train": train_payload,
        "last": last,
    }


# ---- Bằng chứng phân tán: HDFS topology/blocks + Spark executors ----
def _docker_exec(args, timeout=30):
    proc = subprocess.run(
        ["docker", "exec"] + args, capture_output=True, text=True,
        timeout=timeout, encoding="utf-8", errors="replace",
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "docker exec thất bại").strip()[:300])
    return proc.stdout


def evidence():
    out = {"hdfs": {"ok": False}, "spark": {"ok": False}}
    # HDFS: topology (danh sách DataNode) + fsck (số block, replication, nơi đặt block)
    try:
        topo = _docker_exec(["namenode", "hdfs", "dfsadmin", "-printTopology"])
        datanodes = []
        for line in topo.splitlines():
            line = line.strip()
            # dạng: "   172.18.0.3:9866 (datanode1.bigdata)" sau dòng "Rack: /default-rack"
            if line and line[0].isdigit() and ":" in line:
                host = line.split()[0].split(":")[0]
                name = line[line.find("(") + 1:line.find(")")] if "(" in line else host
                datanodes.append(f"{name} ({host})")
        fsck = _docker_exec(
            ["namenode", "hdfs", "fsck", "/mua", "-files", "-blocks", "-locations"],
            timeout=60,
        )
        files, cur = [], None
        for line in fsck.splitlines():
            raw = line
            line = line.strip()
            if not line or line.startswith(("FSCK", "Status:", "FSCK ended")):
                continue
            if line.startswith("/mua/") and "<dir>" in line:
                continue
            if line.startswith("/mua/") and " bytes," in line:
                # /mua/ratings.jsonl 56126625 bytes, replicated: replication=3, 1 block(s): ...
                head = line.split(" bytes,", 1)[0].split()
                path, size = head[0], int(head[1])
                repl = 0
                if "replication=" in line:
                    repl = int(line.split("replication=", 1)[1].split(",")[0])
                cur = {"path": path, "size_mb": round(size / (1024 * 1024), 2),
                       "repl_configured": repl, "blocks": []}
                files.append(cur)
            elif cur is not None and raw[:1].isdigit() and ". BP-" in line:
                # "0. BP-...:blk_1073741831_1007 len=2699147 Live_repl=2 [DatanodeInfoWithStorage[...]]"
                blk = {"id": line.split(":blk_")[1].split()[0] if ":blk_" in line else "?"}
                if "len=" in line:
                    blk["len_mb"] = round(int(line.split("len=", 1)[1].split()[0]) / (1024 * 1024), 2)
                if "Live_repl=" in line:
                    blk["live_repl"] = int(line.split("Live_repl=", 1)[1].split()[0])
                if "DatanodeInfoWithStorage[" in line:
                    nodes = []
                    for part in line.split("DatanodeInfoWithStorage[")[1:]:
                        nodes.append(part.split(":")[0].strip())
                    blk["nodes"] = nodes
                cur["blocks"].append(blk)
        out["hdfs"] = {
            "ok": True,
            "datanodes": datanodes or ["(chưa đọc được topology)"],
            "files": files,
            "file_count": len(files),
            "block_count": sum(len(f["blocks"]) for f in files),
        }
    except Exception as exc:
        out["hdfs"] = {"ok": False, "error": str(exc)}
    # Spark: master + danh sách executor (driver + 2 task threads trên 2 worker)
    try:
        with urlopen(SPARK_MASTER, timeout=5) as response:
            data = json.load(response)
        with _train_lock:
            live = list(_train["executors"])
        out["spark"] = {
            "ok": True,
            "workers": data.get("aliveworkers", 0),
            "cores": data.get("cores", 0),
            "memory": data.get("memory", 0),
            "executors": live,
        }
    except Exception as exc:
        out["spark"] = {"ok": False, "error": str(exc)}
    with _train_lock:
        out["rank"] = _train["rank"]
        out["train_state"] = _train["state"]
        out["serving_users"] = _train["serving_users"]
    return out


# ---- Áp model: export Top-10 live -> nạp MongoDB -> trả diff cho UI ----
_apply_lock = threading.Lock()
_apply = {"state": "idle", "result": None, "error": None, "started": None}


def apply_model():
    if not _apply_lock.acquire(blocking=False):
        return {"ok": False, "error": "Đang có tiến trình áp model khác."}
    try:
        if _apply["state"] == "running":
            return {"ok": False, "error": "Đang áp model — đợi hoàn tất."}
        _apply.update(state="running", result=None, error=None, started=time.time())
        try:
            proc = subprocess.run(
                [sys.executable, str(APPLY_TOOL)],
                capture_output=True, text=True, timeout=420,
                encoding="utf-8", errors="replace", cwd=str(CODE_DIR),
            )
            # tool in JSON cuối stdout, log ra stderr
            last_json = None
            for line in reversed((proc.stdout or "").strip().splitlines()):
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        last_json = json.loads(line)
                        break
                    except json.JSONDecodeError:
                        continue
            if last_json is None:
                _apply.update(state="error", error=(proc.stderr or "apply không có JSON kết quả")[-500:])
                return {"ok": False, "error": _apply["error"]}
            _apply.update(state="done" if last_json.get("ok") else "error",
                          result=last_json if last_json.get("ok") else None,
                          error=None if last_json.get("ok") else last_json.get("error"))
            return last_json
        except subprocess.TimeoutExpired:
            _apply.update(state="error", error="Quá 420s — dừng tiến trình áp model.")
            return {"ok": False, "error": _apply["error"]}
        except Exception as exc:
            _apply.update(state="error", error=str(exc))
            return {"ok": False, "error": str(exc)}
    finally:
        _apply_lock.release()


def apply_status():
    return {"state": _apply["state"], "result": _apply["result"], "error": _apply["error"]}


class Handler(BaseHTTPRequestHandler):
    def _send(self, body, code=200):
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/status":
            self._send(status())
        elif parsed.path == "/api/train/status":
            self._send(train_status())
        elif parsed.path == "/api/evidence":
            self._send(evidence())
        elif parsed.path == "/api/train/apply/status":
            self._send(apply_status())
        else:
            payload = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

    def do_POST(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path == "/api/train/start":
            try:
                rank = int(query.get("rank", ["5"])[0])
            except (TypeError, ValueError):
                rank = 5
            self._send(train_start(rank))
        elif parsed.path == "/api/train/reset":
            self._send(train_reset())
        elif parsed.path == "/api/train/apply":
            self._send(apply_model())
        else:
            self._send({"ok": False, "error": "not found"}, 404)

    def log_message(self, *_args):
        return

print(f"Stream dashboard: http://localhost:{PORT}")
ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
