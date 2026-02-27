import asyncio
import json
import os
import re
import signal
import socket
import sqlite3
import ssl
import sys
import time
import threading
import queue
import readchar
from io import StringIO
from urllib.request import Request, urlopen
from urllib.error import URLError
from aiohttp import web, ClientSession, ClientTimeout, ClientError
from rich.console import Console
from rich.table import Table

_log_lock = threading.Lock()
_token_lock = threading.Lock()
_state_lock = threading.Lock()
_retry_counter = 0
_headless = False


# ─── Константы ─────────────────────────────���─────────────────
LOCAL_PORT = 8080
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klod.db")
HOP_HEADERS = ("Transfer-Encoding", "Connection", "Keep-Alive", "Upgrade")
STRIP_RESP_HEADERS = ("Transfer-Encoding", "Connection", "Content-Encoding")
_KEYS_UP = {readchar.key.UP, "\xe0H"}
_KEYS_DOWN = {readchar.key.DOWN, "\xe0P"}
_ssl_ctx = ssl.create_default_context()


def get_retry_delay(retry_count: int) -> int:
    """Calculate retry delay with exponential backoff: 1, 3, 5, 10, 15+ seconds."""
    delays = [1, 3, 5, 10, 15]
    if retry_count <= len(delays):
        return delays[retry_count - 1]
    return 15

# ─── Состояние ───────────────────────────────────────────────
console = Console()
runner = None
loop = None
total_input_tokens = 0
total_output_tokens = 0
providers: list[dict] = []
provider_index = 0
error_log: list[str] = []
MAX_ERRORS = 10
screen_dirty = False
today_input_tokens = 0
today_output_tokens = 0
today_cache_time = 0
# Активные ретраи: {retry_id: {"provider": str, "count": int, "start": float, "log_idx": int}}
active_retries: dict[int, dict] = {}
proxy_mode = "failover-robin"  # "failover-robin" | "round-robin" | "single"
_proxy_online = False
single_provider_id: int = 0
client_session: ClientSession = None


def _trim_error_log():
    """Вызывать под _log_lock."""
    while len(error_log) > MAX_ERRORS:
        error_log.pop(0)
        for info in active_retries.values():
            info["log_idx"] -= 1


def refresh_today_tokens():
    global today_input_tokens, today_output_tokens, today_cache_time
    now = time.time()
    # Обновляем каждую минуту
    current_slot = int(now) // 60
    with _token_lock:
        cached_slot = int(today_cache_time) // 60 if today_cache_time else -1
    if current_slot == cached_slot:
        return
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) FROM token_log WHERE DATE(ts) = DATE('now')"
        ).fetchone()
    with _token_lock:
        today_input_tokens, today_output_tokens = row[0], row[1]
        today_cache_time = now


def log_error(msg: str, style: str = "red"):
    global screen_dirty
    ts = time.strftime("%H:%M:%S")
    with _log_lock:
        error_log.append(f"[{style}][{ts}] {msg}[/{style}]")
        _trim_error_log()
    screen_dirty = True
    if _headless:
        print(f"[{ts}] {msg}", flush=True)


# ─── База данных ─────────────────────────────────────────────
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS providers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                key TEXT NOT NULL,
                active INTEGER DEFAULT 1,
                expired INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS token_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                model TEXT DEFAULT '',
                provider_id INTEGER DEFAULT 0,
                method TEXT,
                path TEXT,
                status INTEGER,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0
            )
        """)
        # миграции
        for col, col_type, default in [("model", "TEXT", "''"), ("provider_id", "INTEGER", "0")]:
            try:
                conn.execute(f"SELECT {col} FROM token_log LIMIT 1")
            except sqlite3.OperationalError:
                conn.execute(f"ALTER TABLE token_log ADD COLUMN {col} {col_type} DEFAULT {default}")
        try:
            conn.execute("SELECT expired FROM providers LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE providers ADD COLUMN expired INTEGER DEFAULT 0")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.commit()


def db_load_providers() -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT id, name, url, key, active, expired FROM providers ORDER BY id").fetchall()
    return [{"id": r[0], "name": r[1], "url": r[2], "key": r[3], "active": bool(r[4]), "expired": bool(r[5])} for r in rows]


def db_add_provider(name: str, url: str, key: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT INTO providers (name, url, key) VALUES (?,?,?)", (name, url, key))
        conn.commit()


def db_remove_provider(pid: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM providers WHERE id=?", (pid,))
        conn.commit()


def db_toggle_provider(pid: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE providers SET active = 1 - active WHERE id=?", (pid,))
        conn.commit()


def db_edit_provider(pid: int, name: str, url: str, key: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE providers SET name=?, url=?, key=?, expired=0 WHERE id=?", (name, url, key, pid))
        conn.commit()


def db_get_setting(key: str, default: str = "") -> str:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def db_set_setting(key: str, value: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)", (key, value))
        conn.commit()


def db_load_totals(provider_id: int = None) -> tuple[int, int]:
    with sqlite3.connect(DB_PATH) as conn:
        if provider_id is not None:
            row = conn.execute(
                "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) FROM token_log WHERE provider_id=?",
                (provider_id,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) FROM token_log"
            ).fetchone()
    return row[0], row[1]


def db_load_totals_all() -> dict[int, tuple[int, int]]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT provider_id, COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) FROM token_log GROUP BY provider_id"
        ).fetchall()
    return {int(r[0]): (r[1], r[2]) for r in rows}


def db_load_by_model() -> list[tuple[str, int, int, int]]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT COALESCE(NULLIF(model,''), 'unknown'),
                   COALESCE(SUM(input_tokens),0),
                   COALESCE(SUM(output_tokens),0),
                   COUNT(*)
            FROM token_log
            GROUP BY COALESCE(NULLIF(model,''), 'unknown')
            ORDER BY SUM(input_tokens) + SUM(output_tokens) DESC
        """).fetchall()
    return rows


def db_load_by_day(limit: int = 7) -> list[tuple[str, int, int, int]]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT DATE(ts),
                   COALESCE(SUM(input_tokens),0),
                   COALESCE(SUM(output_tokens),0),
                   COUNT(*)
            FROM token_log
            WHERE DATE(ts) >= DATE('now', ?)
            GROUP BY DATE(ts)
            ORDER BY DATE(ts) DESC
        """, (f"-{limit} days",)).fetchall()
    return rows


def db_load_by_hour_today() -> list[tuple[int, int, int, int]]:
    """Load token usage by hour for today. Returns list of (hour, input_tokens, output_tokens, request_count)."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT CAST(strftime('%H', ts) AS INTEGER),
                   COALESCE(SUM(input_tokens),0),
                   COALESCE(SUM(output_tokens),0),
                   COUNT(*)
            FROM token_log
            WHERE DATE(ts) = DATE('now')
            GROUP BY strftime('%H', ts)
            ORDER BY strftime('%H', ts)
        """).fetchall()
    return rows


def db_load_by_provider() -> list[tuple[int, str, int, int, int]]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT t.provider_id,
                   COALESCE(p.name, 'unknown'),
                   COALESCE(SUM(t.input_tokens),0),
                   COALESCE(SUM(t.output_tokens),0),
                   COUNT(*)
            FROM token_log t
            LEFT JOIN providers p ON t.provider_id = p.id
            GROUP BY t.provider_id
            ORDER BY SUM(t.input_tokens) + SUM(t.output_tokens) DESC
        """).fetchall()
    return rows


def db_save(model: str, provider_id: int, method: str, path: str, status: int, inp: int, out: int):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO token_log (ts,model,provider_id,method,path,status,input_tokens,output_tokens) VALUES (?,?,?,?,?,?,?,?)",
                (time.strftime("%Y-%m-%d %H:%M:%S"), model, provider_id, method, path, status, inp, out),
            )
            conn.commit()
    except Exception as e:
        log_error(f"db_save: {e}")


# ─── Провайдеры ──────────────────────────────────────────────
YUNYI_DOMAINS = ("yunyi.cfd", "yunyi.rdzhvip.com", "cdn1.yunyi.cfd", "cdn2.yunyi.cfd")


def is_yunyi(prov: dict) -> bool:
    host = prov["url"].replace("https://", "").replace("http://", "").split("/")[0]
    return host in YUNYI_DOMAINS


def get_active_providers() -> list[dict]:
    return [p for p in providers if p["active"] and not p.get("expired")]


def current_provider() -> dict | None:
    """Return current failover-robin provider without advancing index."""
    with _state_lock:
        active = get_active_providers()
        if not active:
            return None
        global provider_index
        provider_index = provider_index % len(active)
        return active[provider_index]


def advance_provider():
    """Switch to the next provider (called on failure)."""
    with _state_lock:
        global provider_index
        provider_index += 1


def next_provider() -> dict | None:
    """Round-robin: return current and advance."""
    with _state_lock:
        active = get_active_providers()
        if not active:
            return None
        global provider_index
        provider_index = provider_index % len(active)
        p = active[provider_index]
        provider_index += 1
        return p


def reload_providers():
    global providers
    new = db_load_providers()
    with _state_lock:
        providers = new


# ─── Парсинг токенов ─────────────────────────────────────────
def extract_tokens(data: dict, ctx: dict):
    global total_input_tokens, total_output_tokens, today_input_tokens, today_output_tokens

    delta_inp = 0
    delta_out = 0

    msg = data.get("message")
    if isinstance(msg, dict):
        model = msg.get("model", "")
        if model:
            ctx["model"] = model
        usage = msg.get("usage")
        if usage:
            delta_inp += usage.get("input_tokens", 0)

    model = data.get("model", "")
    if model:
        ctx["model"] = model

    usage = data.get("usage")
    if usage:
        # top-level usage may duplicate message.usage input_tokens — only take output here
        if not delta_inp:
            delta_inp += usage.get("input_tokens", 0)
        delta_out += usage.get("output_tokens", 0)

    if delta_inp or delta_out:
        with _token_lock:
            total_input_tokens += delta_inp
            total_output_tokens += delta_out
            today_input_tokens += delta_inp
            today_output_tokens += delta_out
        ctx["inp"] += delta_inp
        ctx["out"] += delta_out


def parse_sse_chunk(chunk: bytes, buf: list, ctx: dict):
    buf.append(chunk.decode("utf-8", errors="replace"))
    text = "".join(buf)
    while "\n\n" in text:
        event, text = text.split("\n\n", 1)
        for line in event.split("\n"):
            if line.startswith("data: "):
                try:
                    extract_tokens(json.loads(line[6:]), ctx)
                except (json.JSONDecodeError, ValueError):
                    pass
    buf.clear()
    if text:
        buf.append(text)


# ─── Прокси ─────────────────────────────────────────────────
async def proxy_handler(request: web.Request) -> web.StreamResponse:
    global screen_dirty
    path = request.path
    query = request.query_string

    headers = dict(request.headers)
    headers.pop("Host", None)
    for h in HOP_HEADERS:
        headers.pop(h, None)

    body = await request.read()
    method = request.method

    active = get_active_providers()
    if not active:
        log_error("No active providers configured", "yellow")
        return web.Response(status=503, text="No active providers configured")

    tried = 0
    total_active = len(active)
    retry_count = 0
    retry_start = None
    retry_id = None

    while True:
        # Refresh active providers on retry rounds
        if tried == 0 and retry_count > 0:
            active = get_active_providers()
            total_active = len(active)
            if not active:
                log_error("No active providers configured", "yellow")
                return web.Response(status=503, text="No active providers configured")

        with _state_lock:
            _mode = proxy_mode
            _single_id = single_provider_id
        if _mode == "single":
            prov = next((p for p in active if p["id"] == _single_id), None)
            if not prov:
                all_prov = next((p for p in providers if p["id"] == _single_id), None)
                if all_prov and not all_prov["active"]:
                    msg = f"Provider '{all_prov['name']}' is disabled. Change mode in Settings or enable the provider."
                    log_error(msg, "yellow")
                    return web.Response(status=503, text=msg)
                log_error(f"Single provider #{_single_id} not found or inactive", "yellow")
                return web.Response(status=503, text="Selected provider not available")
        elif _mode == "failover-robin":
            prov = current_provider()
        else:
            prov = next_provider()
        if not prov:
            log_error("No active providers configured", "yellow")
            return web.Response(status=503, text="No active providers configured")

        target_url = f"{prov['url']}{path}{'?' + query if query else ''}"
        headers["x-api-key"] = prov["key"]
        ctx = {"inp": 0, "out": 0, "model": ""}

        try:
            async with client_session.request(method=method, url=target_url, headers=headers, data=body, ssl=_ssl_ctx) as resp:
                if resp.status >= 400:
                    err_body = await resp.read()
                    err_text = err_body.decode("utf-8", errors="replace")

                    is_expired = resp.status == 401 and "expired" in err_text.lower()
                    if is_expired:
                        prov["expired"] = True
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.execute("UPDATE providers SET expired = 1 WHERE id = ?", (prov["id"],))
                        log_error(f"{prov['name']}: API key expired", "yellow")
                        screen_dirty = True
                        if _mode != "single":
                            advance_provider()
                        tried += 1
                        if tried < total_active:
                            continue
                        return web.Response(status=resp.status, headers={k: v for k, v in resp.headers.items() if k not in STRIP_RESP_HEADERS}, body=err_body)

                    # Check for retryable errors with regex
                    is_no_accounts = bool(re.search(r"no available.*accounts.*support.*model", err_text, re.IGNORECASE))
                    is_cooldown = bool(re.search(r"all apis.*in cooldown", err_text, re.IGNORECASE))
                    is_retryable = is_no_accounts or is_cooldown

                    if is_retryable:
                        # Prepare error text for retry logs
                        err_clean = err_text.strip()
                        if err_clean.lower().startswith("error:"):
                            err_clean = err_clean[6:].strip()
                        reason = err_clean  # No truncation

                        # In single mode, don't try other providers — go straight to retry
                        if _mode == "single":
                            tried = total_active  # skip provider switching

                        if tried < total_active:
                            # Try next provider first
                            advance_provider()
                            tried += 1

                            if tried < total_active:
                                log_error(f"{prov['name']}: {reason}, switching to next provider", "yellow")
                                screen_dirty = True
                                continue

                        # All providers tried — start retry cycle
                        tried = 0
                        if retry_count == 0:
                            retry_start = time.time()
                            retry_ts = time.strftime("%H:%M:%S")
                            with _log_lock:
                                global _retry_counter
                                _retry_counter += 1
                                retry_id = _retry_counter
                                error_log.append("")
                                _trim_error_log()
                                active_retries[retry_id] = {
                                    "provider": prov["name"] if _mode == "single" else "all", "count": 0, "start": retry_start,
                                    "ts": retry_ts, "reason": reason, "log_idx": len(error_log) - 1,
                                }
                        retry_count += 1
                        delay = get_retry_delay(retry_count)
                        with _log_lock:
                            info = active_retries[retry_id]
                            info["count"] = retry_count
                            info["next_retry"] = time.time() + delay
                            idx = info["log_idx"]
                            if 0 <= idx < len(error_log):
                                error_log[idx] = f"[yellow][{info['ts']}] ⟳ {info['provider']}: {info['reason']}: retry #{retry_count}, next in {delay}s[/yellow]"
                        screen_dirty = True
                        await asyncio.sleep(delay)
                        continue

                    log_error(f"{resp.status} {prov['name']}: {err_text[:80]}")
                    return web.Response(
                        status=resp.status,
                        headers={k: v for k, v in resp.headers.items() if k not in STRIP_RESP_HEADERS},
                        body=err_body,
                    )

                # Success — clear retry info if any
                if retry_count > 0:
                    elapsed = int(time.time() - retry_start)
                    with _log_lock:
                        info = active_retries.pop(retry_id, None)
                        if info:
                            idx = info["log_idx"]
                            if 0 <= idx < len(error_log):
                                error_log[idx] = f"[green][{info['ts']}] ✓ {prov['name']}: OK after {retry_count} retry rounds ({elapsed}s)[/green]"
                    screen_dirty = True

                is_stream = "text/event-stream" in resp.headers.get("Content-Type", "")
                resp_headers = {k: v for k, v in resp.headers.items() if k not in STRIP_RESP_HEADERS}

                if is_stream:
                    stream_resp = web.StreamResponse(status=resp.status, headers=resp_headers)
                    try:
                        await stream_resp.prepare(request)
                        sse_buf = []
                        async for chunk in resp.content.iter_any():
                            parse_sse_chunk(chunk, sse_buf, ctx)
                            await stream_resp.write(chunk)
                        await stream_resp.write_eof()
                    except (ConnectionResetError, ConnectionError, BrokenPipeError, ClientError):
                        pass
                    if ctx["inp"] or ctx["out"]:
                        await loop.run_in_executor(None, db_save, ctx["model"], prov["id"], method, path, resp.status, ctx["inp"], ctx["out"])
                    return stream_resp
                else:
                    resp_body = await resp.read()
                    try:
                        extract_tokens(json.loads(resp_body), ctx)
                    except (json.JSONDecodeError, ValueError):
                        pass
                    if ctx["inp"] or ctx["out"]:
                        await loop.run_in_executor(None, db_save, ctx["model"], prov["id"], method, path, resp.status, ctx["inp"], ctx["out"])
                    return web.Response(status=resp.status, headers=resp_headers, body=resp_body)
        except (ClientError, asyncio.TimeoutError, OSError) as e:
            # Network error — treat as retryable
            reason = f"Network error ({type(e).__name__})"
            log_error(f"{prov['name']}: {type(e).__name__}: {e}")

            # In single mode, don't try other providers — go straight to retry
            if _mode == "single":
                tried = total_active  # skip provider switching

            if tried < total_active:
                advance_provider()
                tried += 1
                if tried < total_active:
                    log_error(f"{prov['name']}: {reason}, switching to next provider", "yellow")
                    screen_dirty = True
                    continue
            tried = 0
            if retry_count == 0:
                retry_start = time.time()
                retry_ts = time.strftime("%H:%M:%S")
                with _log_lock:
                    _retry_counter += 1
                    retry_id = _retry_counter
                    error_log.append("")
                    _trim_error_log()
                    active_retries[retry_id] = {
                        "provider": prov["name"] if _mode == "single" else "all", "count": 0, "start": retry_start,
                        "ts": retry_ts, "reason": reason, "log_idx": len(error_log) - 1,
                    }
            retry_count += 1
            delay = get_retry_delay(retry_count)
            with _log_lock:
                info = active_retries[retry_id]
                info["count"] = retry_count
                info["next_retry"] = time.time() + delay
                idx = info["log_idx"]
                if 0 <= idx < len(error_log):
                    error_log[idx] = f"[yellow][{info['ts']}] ⟳ {info['provider']}: {info['reason']}: retry #{retry_count}, next in {delay}s[/yellow]"
            screen_dirty = True
            await asyncio.sleep(delay)
            continue
        except Exception as e:
            log_error(f"{prov['name']}: unexpected {type(e).__name__}: {e}")
            return web.Response(status=502, body=b"Bad Gateway")


# ─── Сервер ──────────────────────────────────────────────────
async def start_server():
    global runner, client_session, _proxy_online
    client_session = ClientSession(timeout=ClientTimeout(total=300, sock_read=300))
    app = web.Application()
    app.router.add_route("*", "/{path_info:.*}", proxy_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", LOCAL_PORT)
    await site.start()
    _proxy_online = True


async def stop_server():
    if runner:
        await runner.cleanup()
    if client_session:
        await client_session.close()
    with _log_lock:
        active_retries.clear()


def run_proxy_loop():
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_server())
    loop.run_forever()


# ─── Ввод ────────────────────────────────────────────────────
_key_queue = queue.Queue()
_key_reader_active = False


def _key_reader():
    """Background thread: reads keys and puts them in queue.
    Uses readchar() + timeout to distinguish bare Esc from arrow sequences."""
    while True:
        try:
            ch = readchar.readchar()
        except EOFError:
            break
        if sys.platform == "win32" and ch in ("\x00", "\xe0"):
            # Windows special keys: arrow keys etc. are two-byte sequences
            try:
                ch2 = readchar.readchar()
                _key_queue.put(ch + ch2)
            except EOFError:
                _key_queue.put(ch)
            continue
        if ch == "\x1b":
            # Could be bare Esc or start of escape sequence (e.g. \x1b[A for Up)
            # Wait briefly for more chars
            import select as _sel
            if sys.platform == "win32":
                _key_queue.put(ch)
                continue
            # On Unix, peek stdin with short timeout
            try:
                r, _, _ = _sel.select([sys.stdin], [], [], 0.05)
            except Exception:
                r = []
            if r:
                # More data available — read the full sequence
                seq = ch
                while True:
                    try:
                        r2, _, _ = _sel.select([sys.stdin], [], [], 0.01)
                    except Exception:
                        break
                    if not r2:
                        break
                    try:
                        seq += readchar.readchar()
                    except EOFError:
                        break
                _key_queue.put(seq)
            else:
                # No more data — bare Esc
                _key_queue.put("\x1b")
        else:
            _key_queue.put(ch)


def _start_key_reader():
    """Start background key reader thread if not already running."""
    global _key_reader_active
    if not _key_reader_active:
        _key_reader_active = True
        threading.Thread(target=_key_reader, daemon=True).start()


def _readkey(timeout: float = None) -> str | None:
    """Read a key from queue (if reader active) or directly."""
    if _key_reader_active:
        try:
            return _key_queue.get(timeout=timeout if timeout else 86400)
        except queue.Empty:
            return None
    return readchar.readkey()


def _is_esc(key: str) -> bool:
    """Check if key is Escape (single or double \\x1b, but not arrow keys)."""
    return key.startswith("\x1b") and key not in (readchar.key.UP, readchar.key.DOWN, readchar.key.LEFT, readchar.key.RIGHT, readchar.key.ENTER)


def read_line(prompt: str = "", prefill: str = "") -> str:
    if prompt:
        console.print(prompt, end="")
    chars = list(prefill)
    if prefill:
        sys.stdout.write(prefill)
        sys.stdout.flush()
    while True:
        key = _readkey()
        if key is None:
            continue
        if key in (readchar.key.ENTER, "\r", "\n"):
            print()
            return "".join(chars).strip()
        elif key in (readchar.key.BACKSPACE, "\x08", "\x7f"):
            if chars:
                chars.pop()
                sys.stdout.write("\b \b")
                sys.stdout.flush()
        elif key == readchar.key.CTRL_C:
            raise KeyboardInterrupt
        elif len(key) > 1:
            continue  # skip special keys (arrows, etc.)
        else:
            chars.append(key)
            sys.stdout.write(key)
            sys.stdout.flush()


def press_any():
    _readkey()


def cls(full: bool = False):
    if full:
        sys.stdout.write("\033[?25l\033[H\033[J")
    else:
        sys.stdout.write("\033[?25l\033[H")
    sys.stdout.flush()


def show_cursor():
    sys.stdout.write("\033[?25h")
    sys.stdout.flush()


def select_option(title: str, options: list[str], selected: int = 0, enter_only: bool = False) -> int | None:
    """Interactive selector with arrow keys. Returns index or None on Esc.
    If enter_only=True, digits only move cursor, confirmation requires Enter."""
    idx = max(0, min(selected, len(options) - 1))
    while True:
        cls(full=True)
        console.print()
        console.print(f"  [bold yellow]{title}[/bold yellow]")
        console.print()
        for i, opt in enumerate(options):
            if i == idx:
                console.print(f"  [bright_yellow]► {opt}[/bright_yellow]")
            else:
                console.print(f"  [dim]  {opt}[/dim]")
        console.print()
        if enter_only:
            console.print(f"  [dim]↑↓ select   Enter confirm   Esc cancel[/dim]")
        else:
            console.print(f"  [dim]↑↓ select   1-{len(options)} jump   Enter confirm   Esc cancel[/dim]")

        while True:
            key = _readkey()
            if key is None:
                continue
            if key in _KEYS_UP:
                idx = (idx - 1) % len(options)
                break
            elif key in _KEYS_DOWN:
                idx = (idx + 1) % len(options)
                break
            elif key in (readchar.key.ENTER, "\r", "\n"):
                return idx
            elif _is_esc(key):
                return None
            elif key == readchar.key.CTRL_C:
                raise KeyboardInterrupt
            elif key.isdigit():
                num = int(key)
                if 1 <= num <= len(options):
                    if enter_only:
                        idx = num - 1
                        break
                    else:
                        return num - 1


# ─── TUI ─────────────────────────────────────────────────────
def fmt(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def bar(value: int, max_value: int, width: int = 20) -> str:
    if max_value == 0:
        return "[dim]" + "░" * width + "[/dim]"
    filled = int(value / max_value * width)
    return "[cyan]" + "█" * filled + "[/cyan][dim]" + "░" * (width - filled) + "[/dim]"


def screen_main():
    buf = StringIO()
    bcon = Console(file=buf, highlight=False, markup=True, width=console.width, force_terminal=True, color_system="truecolor")

    def _p(text: str = ""):
        bcon.print(text, end="")
        buf.write("\033[K\n")

    refresh_today_tokens()
    status = "[on green] [/on green] [bold green]Online[/bold green]" if _proxy_online else "[on red] [/on red] [bold red]Offline[/bold red]"
    with _state_lock:
        _providers = list(providers)
        _proxy_mode = proxy_mode
        _single_id = single_provider_id
    active = [p for p in _providers if p["active"]]
    with _token_lock:
        _today_inp = today_input_tokens
        _today_out = today_output_tokens
    today_total = _today_inp + _today_out

    _p()
    _p(f"  [bold magenta]====[/bold magenta] [bold bright_white]KLOD PROXY[/bold bright_white] [bold magenta]====[/bold magenta]")
    _p()
    _p(f"  {status}   [dim]|[/dim]   [bright_blue]http://localhost:{LOCAL_PORT}[/bright_blue]")
    if _proxy_mode == "single":
        sp = next((p for p in _providers if p["id"] == _single_id), None)
        sp_name = sp["name"] if sp else "?"
        mode_str = f"single [bright_yellow]({sp_name})[/bright_yellow]"
    else:
        mode_str = _proxy_mode
    _p(f"  [bright_white]{len(active)}[/bright_white] [dim]active providers / {len(_providers)} total[/dim]   [dim]mode:[/dim] [bright_white]{mode_str}[/bright_white]")
    _p()
    # Provider statuses
    if _providers:
        if _proxy_mode == "single":
            cur_id = _single_id
        else:
            cur = current_provider()
            cur_id = cur["id"] if cur else None
        totals_map = db_load_totals_all()
        for p in _providers:
            if p.get("expired"):
                marker = "[bold yellow]●[/bold yellow]"
                status_tag = " [yellow]expired[/yellow]"
            elif p["active"]:
                marker = "[bold bright_green]●[/bold bright_green]"
                status_tag = ""
            else:
                marker = "[bold bright_red]●[/bold bright_red]"
                status_tag = ""
            arrow = " [bright_yellow]◄[/bright_yellow]" if p["id"] == cur_id else ""
            t_inp, t_out = totals_map.get(p["id"], (0, 0))
            tokens = fmt(t_inp + t_out)
            # short url: just host
            short_url = p["url"].replace("https://", "").replace("http://", "").split("/")[0]
            _p(f"    {marker} [bright_white]{p['name']}[/bright_white]{status_tag}  [dim]{short_url}[/dim]  [yellow]{tokens}[/yellow]{arrow}")
        _p()
    s_inp = fmt(_today_inp).rjust(8)
    s_out = fmt(_today_out).rjust(8)
    s_total = fmt(today_total).rjust(8)
    _p(f"  [bold bright_cyan]IN[/bold bright_cyan]    [bright_cyan]{s_inp}[/bright_cyan]  [dim]|[/dim]  [bold bright_green]OUT[/bold bright_green]  [bright_green]{s_out}[/bright_green]")
    _p(f"  [bold bright_yellow]TOTAL[/bold bright_yellow] [bright_yellow]{s_total}[/bright_yellow]  [dim]|  today[/dim]")
    _p()
    # Обновляем анимацию активных ретраев прямо в error_log
    with _log_lock:
        for info in active_retries.values():
            idx = info["log_idx"]
            if 0 <= idx < len(error_log):
                next_retry = info.get("next_retry", 0)
                countdown = max(0, int(next_retry - time.time()))
                dots = "." * ((countdown % 3) + 1) + " " * (2 - (countdown % 3))
                error_log[idx] = f"[yellow][{info['ts']}] ⟳ {info.get('provider', 'all')}: {info.get('reason', 'unavailable')}: retry #{info['count']}, next in {countdown}s{dots}[/yellow]"
        log_snapshot = list(error_log[-10:])
    if log_snapshot:
        _p(f"  [bold red]── Log ──[/bold red]")
        for line in log_snapshot:
            _p(f"  {line}")
        _p()
    _p(f"  [dim]{'-' * 46}[/dim]")
    _p(f"  [bold bright_cyan](1)[/bold bright_cyan] Providers  [dim]|[/dim]  [bold bright_green](2)[/bold bright_green] Stats  [dim]|[/dim]  [bold bright_yellow](3)[/bold bright_yellow] Settings  [dim]|[/dim]  [bold red](0)[/bold red] Clear")
    _p()

    # Один write — ноль мерцания
    sys.stdout.write("\033[?25l\033[H\033[J" + buf.getvalue())
    sys.stdout.flush()


def wait_key() -> str:
    """Wait for a single keypress, return the character. Returns ESC as \\x1b."""
    while True:
        key = _readkey()
        if key is None:
            continue
        if key == readchar.key.CTRL_C:
            raise KeyboardInterrupt
        if _is_esc(key):
            return "\x1b"
        if len(key) > 1:
            continue  # skip special keys
        return key


def screen_providers():
    sel = 0
    while True:
        cls(full=True)
        reload_providers()
        console.print()
        console.print(f"  [bold cyan]Providers[/bold cyan]")
        console.print()
        if providers:
            totals_map = db_load_totals_all()
            for i, p in enumerate(providers):
                masked = p["key"][:8] + "••••" if len(p["key"]) > 8 else p["key"]
                st = "[green]ON[/green]" if p["active"] else "[red]OFF[/red]"
                t_inp, t_out = totals_map.get(p["id"], (0, 0))
                short_url = p["url"].replace("https://", "").replace("http://", "").split("/")[0]
                line = f"{i+1}. {p['name']}  [dim]{short_url}[/dim]  {st}  [yellow]{fmt(t_inp + t_out)}[/yellow]"
                if i == sel:
                    console.print(f"  [bright_yellow]► {line}[/bright_yellow]")
                else:
                    console.print(f"  [dim]  {line}[/dim]")
        else:
            console.print("  [dim]No providers. Add one to start proxying.[/dim]")
        console.print()
        console.print(f"  [dim]↑↓ select   1-{len(providers)} jump   [bold bright_cyan]A[/bold bright_cyan] Add   [bold bright_green]E[/bold bright_green] Edit   [bold bright_yellow]S[/bold bright_yellow] On/Off   [bold red]D[/bold red] Delete   Esc back[/dim]")

        while True:
            key = _readkey()
            if key is None:
                continue
            if key in _KEYS_UP and providers:
                sel = (sel - 1) % len(providers)
                break
            elif key in _KEYS_DOWN and providers:
                sel = (sel + 1) % len(providers)
                break
            elif key.lower() in ("e", "у") and providers:
                # Edit selected provider
                p = providers[sel]
                cls(full=True)
                show_cursor()
                console.print()
                console.print(f"  [bold cyan]Edit: {p['name']}[/bold cyan]")
                console.print(f"  [dim]Press Enter to keep current value[/dim]")
                console.print()
                name = read_line(f"  Name: ", prefill=p["name"]) or p["name"]
                url = read_line(f"  URL: ", prefill=p["url"]) or p["url"]
                if not url.startswith("http://") and not url.startswith("https://"):
                    console.print("  [red]URL must start with http:// or https://[/red]")
                    press_any()
                else:
                    pkey = read_line(f"  Key: ", prefill=p["key"]) or p["key"]
                    db_edit_provider(p["id"], name, url, pkey)
                    reload_providers()
                    console.print("  [green]Updated.[/green]")
                    press_any()
                break
            elif key.lower() in ("s", "ы") and providers:
                # Toggle selected provider
                p = providers[sel]
                db_toggle_provider(p["id"])
                reload_providers()
                break
            elif key.lower() in ("d", "в") and providers:
                # Delete selected provider
                p = providers[sel]
                confirm = select_option(f"Delete {p['name']}?", ["1. Yes", "0. No"], selected=1, enter_only=True)
                if confirm == 0:
                    db_remove_provider(p["id"])
                    reload_providers()
                    if sel >= len(providers):
                        sel = max(0, len(providers) - 1)
                break
            elif key.lower() in ("a", "ф"):
                # Add new provider
                cls(full=True)
                show_cursor()
                console.print()
                console.print(f"  [bold cyan]Add provider[/bold cyan]")
                console.print()
                name = read_line("  Name: ")
                if not name:
                    break
                url = read_line("  API URL: ")
                if not url:
                    break
                if not url.startswith("http://") and not url.startswith("https://"):
                    console.print("  [red]URL must start with http:// or https://[/red]")
                    press_any()
                    break
                pkey = read_line("  API key: ")
                if not pkey:
                    break
                db_add_provider(name, url, pkey)
                reload_providers()
                console.print("  [green]Added.[/green]")
                press_any()
                break
            elif _is_esc(key):
                return
            elif key == readchar.key.CTRL_C:
                raise KeyboardInterrupt
            elif key.isdigit() and providers:
                num = int(key)
                if 1 <= num <= len(providers):
                    sel = num - 1
                    break
                reload_providers()


def _yunyi_base_url(prov: dict) -> str:
    """Strip /claude or /codex suffix to get yunyi base URL."""
    url = prov["url"].rstrip("/")
    for suffix in ("/claude", "/codex"):
        if url.endswith(suffix):
            url = url[:-len(suffix)]
            break
    return url


def fetch_yunyi_stats(prov: dict) -> dict | None:
    """Fetch stats from yunyi /user/api/v1/me endpoint. Returns parsed JSON or None."""
    if not is_yunyi(prov):
        return None
    base_url = _yunyi_base_url(prov)
    try:
        ctx = ssl.create_default_context()
        req = Request(f"{base_url}/user/api/v1/me", headers={
            "Authorization": f"Bearer {prov['key']}",
            "User-Agent": "yunyi-activator/1.8.2",
            "Content-Type": "application/json",
        })
        with urlopen(req, timeout=10, context=ctx) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def render_yunyi_stats(prov: dict, data: dict):
    """Render yunyi provider stats to console."""
    status = data.get("status", "?")
    st_color = "green" if status == "active" else "red"
    billing = data.get("billing_type", "?")
    console.print(f"    [bright_white]{prov['name']}[/bright_white]  [{st_color}]{status}[/{st_color}]  [dim]{billing}[/dim]")

    ts = data.get("timestamps", {})
    if ts.get("expires_at"):
        console.print(f"    [dim]expires:[/dim] {ts['expires_at']}")

    q = data.get("quota", {})
    if billing == "duration":
        daily = q.get("daily_quota", 0)
        spent = q.get("daily_spent", 0)
        remain = max(0, daily - spent)
        pct = round(spent / daily * 100) if daily > 0 else 0
        console.print(f"    [dim]daily:[/dim] {fmt(spent)} / {fmt(daily)}  [dim]remaining:[/dim] [green]{fmt(remain)}[/green]  [dim]({pct}% used)[/dim]")
    elif billing == "quota":
        total = q.get("total_quota", 0)
        remaining = q.get("remaining_quota") if isinstance(q.get("remaining_quota"), (int, float)) else max(0, total - q.get("used_quota", 0))
        pct = round((total - remaining) / total * 100) if total > 0 else 0
        console.print(f"    [dim]quota:[/dim] {fmt(int(remaining))} / {fmt(total)}  [dim]({pct}% used)[/dim]")
    elif billing == "count":
        total = q.get("max_requests", 0)
        remaining = q.get("remaining_count") if isinstance(q.get("remaining_count"), (int, float)) else max(0, total - q.get("request_count", 0))
        pct = round((total - remaining) / total * 100) if total > 0 else 0
        console.print(f"    [dim]requests:[/dim] {int(remaining)} / {total}  [dim]({pct}% used)[/dim]")

    usage = data.get("usage", {})
    if usage:
        reqs = usage.get("request_count", 0)
        total_tokens = usage.get("total_tokens", 0)
        console.print(f"    [dim]total tokens:[/dim] [yellow]{fmt(total_tokens)}[/yellow]  [dim]requests:[/dim] {reqs}")


def screen_stats():
    while True:
        cls(full=True)
        console.print()
        console.print(f"  [bold cyan]Statistics[/bold cyan]")
        console.print()

        # Quick summary
        t_inp, t_out = db_load_totals()
        t_total = t_inp + t_out
        with _token_lock:
            today_inp = today_input_tokens
            today_out = today_output_tokens
        today_total = today_inp + today_out

        console.print(f"  [dim]All time:[/dim]  [cyan]{fmt(t_inp)}[/cyan] in  [green]{fmt(t_out)}[/green] out  [yellow]{fmt(t_total)}[/yellow] total")
        console.print(f"  [dim]Today:[/dim]     [cyan]{fmt(today_inp)}[/cyan] in  [green]{fmt(today_out)}[/green] out  [yellow]{fmt(today_total)}[/yellow] total")
        console.print()
        console.print(f"  [bold bright_cyan](1)[/bold bright_cyan] Models     — usage by model")
        console.print(f"  [bold bright_green](2)[/bold bright_green] Providers  — usage by provider + API status")
        console.print(f"  [bold bright_yellow](3)[/bold bright_yellow] Timeline   — daily & hourly breakdown")
        console.print(f"  [bold red](0)[/bold red] Reset      — clear all statistics")
        console.print()
        console.print(f"  [dim]Esc[/dim] Back")
        console.print()

        ch = wait_key()
        if ch == "\x1b":
            break
        elif ch == "1":
            screen_stats_models()
        elif ch == "2":
            screen_stats_providers()
        elif ch == "3":
            screen_stats_timeline()
        elif ch == "0":
            confirm = select_option("Clear all statistics?", ["1. Yes", "0. No"], selected=1, enter_only=True)
            if confirm == 0:
                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute("DELETE FROM token_log")
                    conn.commit()
                cls(full=True)
                console.print()
                console.print("  [green]Statistics cleared.[/green]")
                press_any()


def screen_stats_models():
    cls(full=True)
    console.print()
    models = db_load_by_model()
    t_inp, t_out = db_load_totals()
    t_total = t_inp + t_out

    console.print(f"  [bold cyan]Models[/bold cyan]")
    console.print()
    if models:
        table = Table(box=None, padding=(0, 2), show_header=True, show_edge=False)
        table.add_column("Model", style="bold", min_width=25)
        table.add_column("Input", justify="right", style="cyan")
        table.add_column("Output", justify="right", style="green")
        table.add_column("Total", justify="right", style="yellow")
        table.add_column("Reqs", justify="right", style="dim")
        table.add_column("", min_width=20)
        for model, inp, out, count in models:
            table.add_row(model, fmt(inp), fmt(out), fmt(inp + out), str(count), bar(inp + out, t_total))
        table.add_row("", "", "", "", "", "")
        table.add_row("[bold]Total[/bold]", f"[bold cyan]{fmt(t_inp)}[/bold cyan]", f"[bold green]{fmt(t_out)}[/bold green]",
                      f"[bold yellow]{fmt(t_total)}[/bold yellow]", f"[bold]{sum(r[3] for r in models)}[/bold]", "")
        console.print(table)
    else:
        console.print("  [dim]No data yet.[/dim]")

    console.print()
    console.print("  [dim]Press any key...[/dim]")
    press_any()


def screen_stats_providers():
    cls(full=True)
    console.print()
    by_prov = db_load_by_provider()

    console.print(f"  [bold cyan]Providers[/bold cyan]")
    console.print()
    if by_prov:
        max_prov = max(r[2] + r[3] for r in by_prov)
        table = Table(box=None, padding=(0, 2), show_header=True, show_edge=False)
        table.add_column("Provider", style="bold", min_width=15)
        table.add_column("Input", justify="right", style="cyan")
        table.add_column("Output", justify="right", style="green")
        table.add_column("Total", justify="right", style="yellow")
        table.add_column("Reqs", justify="right", style="dim")
        table.add_column("", min_width=20)
        for _, name, inp, out, count in by_prov:
            table.add_row(name, fmt(inp), fmt(out), fmt(inp + out), str(count), bar(inp + out, max_prov))
        console.print(table)
    else:
        console.print("  [dim]No data yet.[/dim]")

    # Yunyi API stats
    yunyi_provs = [p for p in providers if is_yunyi(p)]
    if yunyi_provs:
        console.print()
        console.print(f"  [bold cyan]API Status[/bold cyan]")
        console.print()
        console.print(f"  [dim]Fetching API stats...[/dim]")
        results = {}
        threads = []
        for p in yunyi_provs:
            def _fetch(prov=p):
                results[prov["id"]] = fetch_yunyi_stats(prov)
            t = threading.Thread(target=_fetch)
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=15)
        # Re-render after fetch
        sys.stdout.write("\033[A\033[K")
        sys.stdout.flush()
        for p in yunyi_provs:
            data = results.get(p["id"])
            if data:
                render_yunyi_stats(p, data)
            else:
                console.print(f"    [bright_white]{p['name']}[/bright_white]  [red]unavailable[/red]")
            console.print()

    console.print()
    console.print("  [dim]Press any key...[/dim]")
    press_any()


def screen_stats_timeline():
    cls(full=True)
    console.print()

    # Last 7 days
    days = db_load_by_day(7)
    console.print(f"  [bold cyan]Last 7 days[/bold cyan]")
    console.print()
    if days:
        max_day = max(r[1] + r[2] for r in days)
        table = Table(box=None, padding=(0, 2), show_header=True, show_edge=False)
        table.add_column("Date", style="bold", min_width=12)
        table.add_column("Input", justify="right", style="cyan")
        table.add_column("Output", justify="right", style="green")
        table.add_column("Total", justify="right", style="yellow")
        table.add_column("Reqs", justify="right", style="dim")
        table.add_column("", min_width=20)
        for date, inp, out, count in days:
            table.add_row(date, fmt(inp), fmt(out), fmt(inp + out), str(count), bar(inp + out, max_day))
        console.print(table)
    else:
        console.print("  [dim]No data yet.[/dim]")

    # Today by hour
    hours = db_load_by_hour_today()
    if hours:
        console.print()
        console.print(f"  [bold cyan]Today by hour[/bold cyan]")
        console.print()
        max_hour = max(r[1] + r[2] for r in hours)
        table = Table(box=None, padding=(0, 2), show_header=True, show_edge=False)
        table.add_column("Hour", style="bold", width=6)
        table.add_column("Input", justify="right", style="cyan")
        table.add_column("Output", justify="right", style="green")
        table.add_column("Total", justify="right", style="yellow")
        table.add_column("Reqs", justify="right", style="dim")
        table.add_column("", min_width=20)
        for hour, inp, out, count in hours:
            table.add_row(f"{hour:02d}:00", fmt(inp), fmt(out), fmt(inp + out), str(count), bar(inp + out, max_hour))
        console.print(table)

    console.print()
    console.print("  [dim]Press any key...[/dim]")
    press_any()


def screen_settings():
    global proxy_mode, single_provider_id
    while True:
        cls(full=True)
        console.print()
        console.print(f"  [bold yellow]Settings[/bold yellow]")
        console.print()
        with _state_lock:
            _mode = proxy_mode
            _single_id = single_provider_id
        if _mode == "failover-robin":
            mode_label = "[green]failover-robin[/green]"
        elif _mode == "round-robin":
            mode_label = "[cyan]round-robin[/cyan]"
        else:
            sp = next((p for p in providers if p["id"] == _single_id), None)
            sp_name = sp["name"] if sp else "?"
            mode_label = f"[yellow]single ({sp_name})[/yellow]"
        console.print(f"  [bold bright_cyan](1)[/bold bright_cyan] Port       [bold]{LOCAL_PORT}[/bold]")
        console.print(f"  [bold bright_green](2)[/bold bright_green] Mode       {mode_label}")
        console.print()
        console.print(f"  [dim]  failover-robin — один провайдер, пока работает. При ошибке — следующий[/dim]")
        console.print(f"  [dim]  round-robin — каждый запрос к следующему провайдеру по кругу[/dim]")
        console.print(f"  [dim]  single      — только один конкретный провайдер[/dim]")
        console.print()
        console.print(f"  [dim]Esc[/dim] Back")
        console.print()
        ch = wait_key()
        if ch == "\x1b":
            break
        elif ch == "1":
            show_cursor()
            val = read_line("  New port: ")
            if val.isdigit():
                db_set_setting("port", val)
                console.print(f"  [yellow]Saved port {val}. Restart proxy to apply.[/yellow]")
                press_any()
        elif ch == "2":
            modes = ["failover-robin", "round-robin", "single"]
            with _state_lock:
                cur_mode = proxy_mode
                cur_single = single_provider_id
            cur_idx = modes.index(cur_mode) if cur_mode in modes else 0
            choice = select_option("Select mode", modes, selected=cur_idx)
            if choice is not None:
                new_mode = modes[choice]
                if new_mode == "single":
                    active = get_active_providers()
                    if not active:
                        cls(full=True)
                        console.print()
                        console.print("  [red]No active providers.[/red]")
                        press_any()
                        continue
                    names = [p["name"] for p in active]
                    cur_sp = next((i for i, p in enumerate(active) if p["id"] == cur_single), 0)
                    sp_choice = select_option("Select provider", names, selected=cur_sp)
                    if sp_choice is None:
                        continue
                    cur_single = active[sp_choice]["id"]
                    db_set_setting("single_provider_id", str(cur_single))
                with _state_lock:
                    proxy_mode = new_mode
                    single_provider_id = cur_single
                db_set_setting("proxy_mode", new_mode)


# ─── Headless ─────────────────────────────────────────────────
def main_headless():
    global _headless, total_input_tokens, total_output_tokens, proxy_mode, single_provider_id, LOCAL_PORT, loop
    _headless = True

    init_db()
    reload_providers()
    old_sticky = db_get_setting("sticky_mode", "")
    if old_sticky and not db_get_setting("proxy_mode", ""):
        db_set_setting("proxy_mode", "failover-robin" if old_sticky == "1" else "round-robin")
    proxy_mode = db_get_setting("proxy_mode", "failover-robin")
    if proxy_mode in ("sticky", "failover"):
        proxy_mode = "failover-robin"
        db_set_setting("proxy_mode", "failover-robin")
    if proxy_mode not in ("failover-robin", "round-robin", "single"):
        proxy_mode = "failover-robin"
    single_provider_id = int(db_get_setting("single_provider_id", "0"))
    LOCAL_PORT = int(db_get_setting("port", str(LOCAL_PORT)))
    total_input_tokens, total_output_tokens = db_load_totals()

    active = get_active_providers()
    print(f"Proxy listening on 0.0.0.0:{LOCAL_PORT}  ({len(active)} active providers, mode: {proxy_mode})", flush=True)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown(sig, frame):
        print(f"\nReceived signal {sig}, shutting down...", flush=True)
        asyncio.run_coroutine_threadsafe(stop_server(), loop)
        loop.call_soon_threadsafe(loop.stop)

    signal.signal(signal.SIGINT, _shutdown)
    if sys.platform != "win32":
        signal.signal(signal.SIGTERM, _shutdown)

    loop.run_until_complete(start_server())
    loop.run_forever()
    loop.close()
    print("Proxy stopped.", flush=True)


# ─── Main ────────────────────────────────────────────────────
def main():
    global total_input_tokens, total_output_tokens, screen_dirty, proxy_mode, single_provider_id, LOCAL_PORT

    # Включаем ANSI escape codes на Windows
    if sys.platform == "win32":
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)

    init_db()
    reload_providers()
    # Миграция: sticky_mode → proxy_mode
    old_sticky = db_get_setting("sticky_mode", "")
    if old_sticky and not db_get_setting("proxy_mode", ""):
        db_set_setting("proxy_mode", "failover-robin" if old_sticky == "1" else "round-robin")
    proxy_mode = db_get_setting("proxy_mode", "failover-robin")
    if proxy_mode in ("sticky", "failover"):
        proxy_mode = "failover-robin"
        db_set_setting("proxy_mode", "failover-robin")
    if proxy_mode not in ("failover-robin", "round-robin", "single"):
        proxy_mode = "failover-robin"
    single_provider_id = int(db_get_setting("single_provider_id", "0"))
    LOCAL_PORT = int(db_get_setting("port", str(LOCAL_PORT)))

    # Check if port is already in use (another instance running)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", LOCAL_PORT)) == 0:
            console.print(f"\n  [bold red]Port {LOCAL_PORT} is already in use.[/bold red]")
            console.print(f"  [dim]Another instance of Klod Proxy is likely running.[/dim]\n")
            sys.exit(1)

    total_input_tokens, total_output_tokens = db_load_totals()
    refresh_today_tokens()

    threading.Thread(target=run_proxy_loop, daemon=True).start()
    _start_key_reader()
    time.sleep(0.3)

    while True:
        screen_main()
        ch = None
        last_refresh = time.time()
        while ch is None:
            now = time.time()
            if screen_dirty or now - last_refresh >= 60:
                screen_dirty = False
                last_refresh = now
                screen_main()
            if active_retries:
                screen_main()
            key = _readkey(timeout=0.5)
            if key is None:
                continue
            if _is_esc(key):
                continue  # ignore Esc on main screen
            if len(key) == 1 and key not in ("\r", "\n"):
                ch = key
        if ch == "1":
            screen_providers()
            reload_providers()
        elif ch == "2":
            screen_stats()
        elif ch == "3":
            screen_settings()
        elif ch == "0":
            with _log_lock:
                error_log.clear()


def cli():
    args = sys.argv[1:]
    if not args:
        try:
            main()
        except KeyboardInterrupt:
            show_cursor()
            console.print("\n  [red]Interrupted.[/red]")
            if loop:
                future = asyncio.run_coroutine_threadsafe(stop_server(), loop)
                try:
                    future.result(timeout=5)
                except Exception:
                    pass
                loop.call_soon_threadsafe(loop.stop)
        return

    cmd = args[0]

    if cmd == "--headless":
        main_headless()

    elif cmd == "add":
        if len(args) < 4:
            print("Usage: proxy.py add <name> <url> <key>")
            sys.exit(1)
        init_db()
        name, url, key = args[1], args[2], args[3]
        if not url.startswith("http://") and not url.startswith("https://"):
            print("Error: URL must start with http:// or https://")
            sys.exit(1)
        db_add_provider(name, url, key)
        print(f"Added provider: {name}")

    elif cmd == "rm":
        if len(args) < 2:
            print("Usage: proxy.py rm <id>")
            sys.exit(1)
        init_db()
        try:
            pid = int(args[1])
        except ValueError:
            print(f"Error: '{args[1]}' is not a valid provider ID")
            sys.exit(1)
        db_remove_provider(pid)
        print(f"Removed provider #{pid}")

    elif cmd == "toggle":
        if len(args) < 2:
            print("Usage: proxy.py toggle <id>")
            sys.exit(1)
        init_db()
        try:
            pid = int(args[1])
        except ValueError:
            print(f"Error: '{args[1]}' is not a valid provider ID")
            sys.exit(1)
        db_toggle_provider(pid)
        print(f"Toggled provider #{pid}")

    elif cmd == "list":
        init_db()
        provs = db_load_providers()
        if not provs:
            print("No providers configured.")
        else:
            for p in provs:
                st = "ON" if p["active"] else "OFF"
                masked = p["key"][:8] + "****" if len(p["key"]) > 8 else p["key"]
                print(f"  #{p['id']}  {st:3s}  {p['name']}  {p['url']}  {masked}")

    else:
        print("Usage: proxy.py [--headless | add | rm | toggle | list]")
        print()
        print("Commands:")
        print("  (no args)    Start TUI")
        print("  --headless   Start proxy without TUI")
        print("  add <name> <url> <key>   Add provider")
        print("  rm <id>                  Remove provider")
        print("  toggle <id>              Toggle provider on/off")
        print("  list                     List providers")
        sys.exit(1)


if __name__ == "__main__":
    cli()
