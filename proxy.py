import asyncio
import json
import msvcrt
import os
import sqlite3
import ssl
import sys
import time
import threading
from io import StringIO
from urllib.request import Request, urlopen
from urllib.error import URLError
from aiohttp import web, ClientSession, ClientTimeout
from rich.console import Console
from rich.table import Table

_log_lock = threading.Lock()
_token_lock = threading.Lock()

# ─── Константы ─────────────────────────────���─────────────────
LOCAL_PORT = 8080
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klod.db")
HOP_HEADERS = ("Transfer-Encoding", "Connection", "Keep-Alive", "Upgrade")
STRIP_RESP_HEADERS = ("Transfer-Encoding", "Connection", "Content-Encoding")
RETRY_DELAY = 5

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
proxy_mode = "sticky"  # "sticky" | "round-robin" | "single"
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
    # Обновляем в фиксированные 5-минутные слоты (xx:00, xx:05, xx:10, ...)
    current_slot = int(now) // 300
    cached_slot = int(today_cache_time) // 300 if today_cache_time else -1
    if current_slot == cached_slot:
        return
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) FROM token_log WHERE DATE(ts) = DATE('now')"
    ).fetchone()
    conn.close()
    today_input_tokens, today_output_tokens = row[0], row[1]
    today_cache_time = now  # флаг для перерисовки из async-потока


def log_error(msg: str, style: str = "red"):
    global screen_dirty
    ts = time.strftime("%H:%M:%S")
    with _log_lock:
        error_log.append(f"[{style}][{ts}] {msg}[/{style}]")
        _trim_error_log()
    screen_dirty = True


# ─── База данных ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS providers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            key TEXT NOT NULL,
            active INTEGER DEFAULT 1
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
    for col, default in [("model", "''"), ("provider_id", "0")]:
        try:
            conn.execute(f"SELECT {col} FROM token_log LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute(f"ALTER TABLE token_log ADD COLUMN {col} TEXT DEFAULT {default}")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def db_load_providers() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT id, name, url, key, active FROM providers ORDER BY id").fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "url": r[2], "key": r[3], "active": bool(r[4])} for r in rows]


def db_add_provider(name: str, url: str, key: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO providers (name, url, key) VALUES (?,?,?)", (name, url, key))
    conn.commit()
    conn.close()


def db_remove_provider(pid: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM providers WHERE id=?", (pid,))
    conn.commit()
    conn.close()


def db_toggle_provider(pid: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE providers SET active = 1 - active WHERE id=?", (pid,))
    conn.commit()
    conn.close()


def db_edit_provider(pid: int, name: str, url: str, key: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE providers SET name=?, url=?, key=? WHERE id=?", (name, url, key, pid))
    conn.commit()
    conn.close()


def db_get_setting(key: str, default: str = "") -> str:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row[0] if row else default


def db_set_setting(key: str, value: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)", (key, value))
    conn.commit()
    conn.close()


def db_load_totals(provider_id: int = None) -> tuple[int, int]:
    conn = sqlite3.connect(DB_PATH)
    if provider_id is not None:
        row = conn.execute(
            "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) FROM token_log WHERE provider_id=?",
            (provider_id,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) FROM token_log"
        ).fetchone()
    conn.close()
    return row[0], row[1]


def db_load_totals_all() -> dict[int, tuple[int, int]]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT provider_id, COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) FROM token_log GROUP BY provider_id"
    ).fetchall()
    conn.close()
    return {int(r[0]): (r[1], r[2]) for r in rows}


def db_load_by_model() -> list[tuple[str, int, int, int]]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT COALESCE(NULLIF(model,''), 'unknown'),
               COALESCE(SUM(input_tokens),0),
               COALESCE(SUM(output_tokens),0),
               COUNT(*)
        FROM token_log
        GROUP BY COALESCE(NULLIF(model,''), 'unknown')
        ORDER BY SUM(input_tokens) + SUM(output_tokens) DESC
    """).fetchall()
    conn.close()
    return rows


def db_load_by_day(limit: int = 7) -> list[tuple[str, int, int, int]]:
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    return rows


def db_load_by_provider() -> list[tuple[int, str, int, int, int]]:
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    return rows


def db_save(model: str, provider_id: int, method: str, path: str, status: int, inp: int, out: int):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO token_log (ts,model,provider_id,method,path,status,input_tokens,output_tokens) VALUES (?,?,?,?,?,?,?,?)",
            (time.strftime("%Y-%m-%d %H:%M:%S"), model, provider_id, method, path, status, inp, out),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log_error(f"db_save: {e}")


# ─── Провайдеры ──────────────────────────────────────────────
YUNYI_DOMAINS = ("yunyi.cfd", "yunyi.rdzhvip.com", "cdn1.yunyi.cfd", "cdn2.yunyi.cfd")


def is_yunyi(prov: dict) -> bool:
    host = prov["url"].replace("https://", "").replace("http://", "").split("/")[0]
    return host in YUNYI_DOMAINS


def get_active_providers() -> list[dict]:
    return [p for p in providers if p["active"]]


def current_provider() -> dict | None:
    """Return current sticky provider without advancing index."""
    active = get_active_providers()
    if not active:
        return None
    global provider_index
    provider_index = provider_index % len(active)
    return active[provider_index]


def advance_provider():
    """Switch to the next provider (called on failure)."""
    global provider_index
    provider_index += 1


def next_provider() -> dict | None:
    """Round-robin: return current and advance."""
    p = current_provider()
    if p:
        advance_provider()
    return p


def reload_providers():
    global providers
    providers = db_load_providers()


# ─── Парсинг токенов ─────────────────────────────────────────
def extract_tokens(data: dict, ctx: dict):
    global total_input_tokens, total_output_tokens, today_input_tokens, today_output_tokens

    msg = data.get("message")
    if isinstance(msg, dict):
        model = msg.get("model", "")
        if model:
            ctx["model"] = model
        usage = msg.get("usage")
        if usage:
            inp = usage.get("input_tokens", 0)
            with _token_lock:
                total_input_tokens += inp
                today_input_tokens += inp
            ctx["inp"] += inp

    model = data.get("model", "")
    if model:
        ctx["model"] = model

    usage = data.get("usage")
    if usage:
        inp = usage.get("input_tokens", 0)
        out = usage.get("output_tokens", 0)
        with _token_lock:
            total_input_tokens += inp
            total_output_tokens += out
            today_input_tokens += inp
            today_output_tokens += out
        ctx["inp"] += inp
        ctx["out"] += out


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
        if proxy_mode == "single":
            prov = next((p for p in active if p["id"] == single_provider_id), None)
            if not prov:
                log_error(f"Single provider #{single_provider_id} not found or inactive", "yellow")
                return web.Response(status=503, text="Selected provider not available")
        elif proxy_mode == "sticky":
            prov = current_provider()
        else:
            prov = next_provider()
        if not prov:
            log_error("No active providers configured", "yellow")
            return web.Response(status=503, text="No active providers configured")

        target_url = f"{prov['url']}{path}{'?' + query if query else ''}"
        headers["x-api-key"] = prov["key"]
        ctx = {"inp": 0, "out": 0, "model": ""}

        async with client_session.request(method=method, url=target_url, headers=headers, data=body, ssl=True) as resp:
            if resp.status >= 400:
                err_body = await resp.read()
                err_text = err_body.decode("utf-8", errors="replace")

                if resp.status == 500 and "no avail" in err_text.lower():
                    # Try next provider first
                    advance_provider()
                    tried += 1

                    if tried < total_active:
                        log_error(f"{prov['name']}: unavailable, switching to next provider", "yellow")
                        screen_dirty = True
                        continue

                    # All providers tried — start retry cycle
                    tried = 0
                    if retry_count == 0:
                        retry_start = time.time()
                        retry_id = id(asyncio.current_task())
                        retry_ts = time.strftime("%H:%M:%S")
                        with _log_lock:
                            error_log.append("")
                            _trim_error_log()
                            active_retries[retry_id] = {
                                "provider": "all", "count": 0, "start": retry_start,
                                "ts": retry_ts, "log_idx": len(error_log) - 1,
                            }
                    retry_count += 1
                    with _log_lock:
                        info = active_retries[retry_id]
                        info["count"] = retry_count
                        idx = info["log_idx"]
                        if 0 <= idx < len(error_log):
                            elapsed = int(time.time() - retry_start)
                            error_log[idx] = f"[yellow][{info['ts']}] ⟳ all providers unavailable: retry round #{retry_count} ({elapsed}s)[/yellow]"
                    screen_dirty = True
                    await asyncio.sleep(RETRY_DELAY)
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
                except (ConnectionResetError, ConnectionError, BrokenPipeError):
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


# ─── Сервер ──────────────────────────────────────────────────
async def start_server():
    global runner, client_session
    client_session = ClientSession(timeout=ClientTimeout(total=300, sock_read=300))
    app = web.Application()
    app.router.add_route("*", "/{path_info:.*}", proxy_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", LOCAL_PORT)
    await site.start()


async def stop_server():
    if runner:
        await runner.cleanup()
    if client_session:
        await client_session.close()


def run_proxy_loop():
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_server())
    loop.run_forever()


# ─── Ввод ────────────────────────────────────────────────────
def read_line(prompt: str = "", prefill: str = "") -> str:
    if prompt:
        console.print(prompt, end="")
    chars = list(prefill)
    if prefill:
        sys.stdout.write(prefill)
        sys.stdout.flush()
    while True:
        if msvcrt.kbhit():
            ch = msvcrt.getwch()
            if ch in ("\x00", "\xe0"):
                msvcrt.getwch()  # consume scan code of extended key
                continue
            if ch in ("\r", "\n"):
                print()
                return "".join(chars).strip()
            elif ch in ("\x08", "\x7f"):
                if chars:
                    chars.pop()
                    sys.stdout.write("\b \b")
                    sys.stdout.flush()
            elif ch == "\x03":
                raise KeyboardInterrupt
            else:
                chars.append(ch)
                sys.stdout.write(ch)
                sys.stdout.flush()
        else:
            time.sleep(0.02)


def press_any():
    while not msvcrt.kbhit():
        time.sleep(0.02)
    msvcrt.getwch()


def cls(full: bool = False):
    if full:
        sys.stdout.write("\033[?25l\033[H\033[J")
    else:
        sys.stdout.write("\033[?25l\033[H")
    sys.stdout.flush()


def show_cursor():
    sys.stdout.write("\033[?25h")
    sys.stdout.flush()


def select_option(title: str, options: list[str], selected: int = 0) -> int | None:
    """Interactive selector with arrow keys. Returns index or None on Esc."""
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
        console.print(f"  [dim]↑↓ select   Enter confirm   Esc cancel[/dim]")

        while True:
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch in ("\x00", "\xe0"):
                    sc = msvcrt.getwch()
                    if sc == "H":  # up
                        idx = (idx - 1) % len(options)
                        break
                    elif sc == "P":  # down
                        idx = (idx + 1) % len(options)
                        break
                elif ch in ("\r", "\n"):
                    return idx
                elif ch == "\x1b":  # Esc
                    return None
                elif ch == "\x03":
                    raise KeyboardInterrupt
            else:
                time.sleep(0.02)


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
    is_online = loop and loop.is_running()
    status = "[on green] [/on green] [bold green]Online[/bold green]" if is_online else "[on red] [/on red] [bold red]Offline[/bold red]"
    active = get_active_providers()
    with _token_lock:
        _today_inp = today_input_tokens
        _today_out = today_output_tokens
    today_total = _today_inp + _today_out

    _p()
    _p(f"  [bold magenta]====[/bold magenta] [bold bright_white]KLOD PROXY[/bold bright_white] [bold magenta]====[/bold magenta]")
    _p()
    _p(f"  {status}   [dim]|[/dim]   [bright_blue]http://localhost:{LOCAL_PORT}[/bright_blue]")
    if proxy_mode == "single":
        sp = next((p for p in providers if p["id"] == single_provider_id), None)
        sp_name = sp["name"] if sp else "?"
        mode_str = f"single [bright_yellow]({sp_name})[/bright_yellow]"
    else:
        mode_str = proxy_mode
    _p(f"  [bright_white]{len(active)}[/bright_white] [dim]active providers / {len(providers)} total[/dim]   [dim]mode:[/dim] [bright_white]{mode_str}[/bright_white]")
    _p()
    # Provider statuses
    if providers:
        cur = current_provider()
        cur_id = cur["id"] if cur else None
        totals_map = db_load_totals_all()
        for p in providers:
            if p["active"]:
                marker = "[bold bright_green]●[/bold bright_green]"
            else:
                marker = "[dim]○[/dim]"
            arrow = " [bright_yellow]◄[/bright_yellow]" if p["id"] == cur_id else ""
            t_inp, t_out = totals_map.get(p["id"], (0, 0))
            tokens = fmt(t_inp + t_out)
            # short url: just host
            short_url = p["url"].replace("https://", "").replace("http://", "").split("/")[0]
            _p(f"    {marker} [bright_white]{p['name']}[/bright_white]  [dim]{short_url}[/dim]  [yellow]{tokens}[/yellow]{arrow}")
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
                elapsed = int(time.time() - info["start"])
                dots = "." * ((elapsed % 3) + 1) + " " * (2 - (elapsed % 3))
                error_log[idx] = f"[yellow][{info['ts']}] ⟳ {info['provider']}: retrying #{info['count']} ({elapsed}s){dots}[/yellow]"
        log_snapshot = list(error_log[-10:])
    if log_snapshot:
        _p(f"  [bold red]── Log ──[/bold red]")
        for line in log_snapshot:
            _p(f"  {line}")
        _p()
    _p(f"  [dim]{'-' * 46}[/dim]")
    _p(f"  [bold bright_cyan](1)[/bold bright_cyan] Providers  [dim]|[/dim]  [bold bright_green](2)[/bold bright_green] Stats  [dim]|[/dim]  [bold bright_yellow](3)[/bold bright_yellow] Settings  [dim]|[/dim]  [bold red](0)[/bold red] Clear")
    _p()
    buf.write("\033[J")

    # Один write — ноль мерцания
    sys.stdout.write("\033[?25l\033[H" + buf.getvalue())
    sys.stdout.flush()


def wait_key() -> str:
    """Wait for a single keypress, return the character. Ignores extended keys."""
    while True:
        if msvcrt.kbhit():
            ch = msvcrt.getwch()
            if ch in ("\x00", "\xe0"):
                msvcrt.getwch()
                continue
            if ch == "\x03":
                raise KeyboardInterrupt
            return ch
        time.sleep(0.02)


def screen_providers():
    while True:
        cls(full=True)
        reload_providers()
        console.print()
        console.print(f"  [bold cyan]Providers[/bold cyan]")
        console.print()
        if providers:
            totals_map = db_load_totals_all()
            table = Table(box=None, padding=(0, 2), show_header=True, show_edge=False)
            table.add_column("#", style="dim", width=4)
            table.add_column("Name", style="bold", min_width=15)
            table.add_column("URL", min_width=30)
            table.add_column("Key", style="dim")
            table.add_column("Status")
            table.add_column("Tokens", justify="right", style="yellow")
            for p in providers:
                masked = p["key"][:8] + "••••" if len(p["key"]) > 8 else p["key"]
                st = "[green]ON[/green]" if p["active"] else "[red]OFF[/red]"
                t_inp, t_out = totals_map.get(p["id"], (0, 0))
                table.add_row(str(p["id"]), p["name"], p["url"], masked, st, fmt(t_inp + t_out))
            console.print(table)
        else:
            console.print("  [dim]No providers. Add one to start proxying.[/dim]")
        console.print()
        console.print(f"  [bold bright_cyan](1)[/bold bright_cyan] Add   [bold bright_green](2)[/bold bright_green] Edit   [bold red](3)[/bold red] Delete   [bold bright_yellow](4)[/bold bright_yellow] On/Off   [dim]Esc[/dim] Back")
        console.print()
        ch = wait_key()
        if ch == "\x1b":
            break
        elif ch == "1":
            cls(full=True)
            show_cursor()
            console.print()
            console.print(f"  [bold cyan]Add provider[/bold cyan]")
            console.print()
            name = read_line("  Name: ")
            if not name:
                continue
            url = read_line("  API URL: ")
            if not url:
                continue
            key = read_line("  API key: ")
            if not key:
                continue
            db_add_provider(name, url, key)
            reload_providers()
            console.print("  [green]Added.[/green]")
            press_any()
        elif ch == "2":
            if not providers:
                continue
            names = [f"{p['id']}. {p['name']}" for p in providers]
            choice = select_option("Edit provider", names)
            if choice is None:
                continue
            old = providers[choice]
            cls(full=True)
            show_cursor()
            console.print()
            console.print(f"  [bold cyan]Edit: {old['name']}[/bold cyan]")
            console.print(f"  [dim]Press Enter to keep current value[/dim]")
            console.print()
            name = read_line(f"  Name: ", prefill=old["name"]) or old["name"]
            url = read_line(f"  URL: ", prefill=old["url"]) or old["url"]
            key = read_line(f"  Key: ", prefill=old["key"]) or old["key"]
            db_edit_provider(old["id"], name, url, key)
            reload_providers()
            console.print("  [green]Updated.[/green]")
            press_any()
        elif ch == "3":
            if not providers:
                continue
            names = [f"{p['id']}. {p['name']}" for p in providers]
            choice = select_option("Delete provider", names)
            if choice is None:
                continue
            target = providers[choice]
            confirm = select_option(f"Delete {target['name']}?", ["1. Yes", "0. No"], selected=1)
            if confirm == 0:
                db_remove_provider(target["id"])
                reload_providers()
        elif ch == "4":
            if not providers:
                continue
            names = [f"{p['id']}. {p['name']}  {'[green]ON[/green]' if p['active'] else '[red]OFF[/red]'}" for p in providers]
            choice = select_option("Toggle provider", names)
            if choice is not None:
                db_toggle_provider(providers[choice]["id"])
                reload_providers()


def fetch_yunyi_stats(prov: dict) -> dict | None:
    """Fetch stats from yunyi /user/api/v1/me endpoint. Returns parsed JSON or None."""
    if not is_yunyi(prov):
        return None
    url = prov["url"].rstrip("/")
    for suffix in ("/claude", "/codex"):
        if url.endswith(suffix):
            url = url[:-len(suffix)]
            break
    try:
        ctx = ssl.create_default_context()
        req = Request(f"{url}/user/api/v1/me", headers={
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

    # Usage stats
    usage = data.get("usage", {})
    if usage:
        reqs = usage.get("request_count", 0)
        total_tokens = usage.get("total_tokens", 0)
        console.print(f"    [dim]total tokens:[/dim] [yellow]{fmt(total_tokens)}[/yellow]  [dim]requests:[/dim] {reqs}")


def screen_stats():
    cls(full=True)
    console.print()

    # По моделям
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

    # По провайдерам
    by_prov = db_load_by_provider()
    if by_prov:
        console.print()
        console.print(f"  [bold cyan]Providers[/bold cyan]")
        console.print()
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

    # По дням
    days = db_load_by_day(7)
    if days:
        console.print()
        console.print(f"  [bold cyan]Last 7 days[/bold cyan]")
        console.print()
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

    # Yunyi API stats
    yunyi_provs = [p for p in providers if is_yunyi(p)]
    if yunyi_provs:
        console.print()
        console.print(f"  [bold cyan]API Status[/bold cyan]")
        console.print()
        for p in yunyi_provs:
            data = fetch_yunyi_stats(p)
            if data:
                render_yunyi_stats(p, data)
            else:
                console.print(f"    [bright_white]{p['name']}[/bright_white]  [red]unavailable[/red]")
            console.print()

    console.print()
    console.print("  [dim]Press any key...[/dim]")
    press_any()


def screen_settings():
    global LOCAL_PORT, proxy_mode, single_provider_id
    while True:
        cls(full=True)
        console.print()
        console.print(f"  [bold yellow]Settings[/bold yellow]")
        console.print()
        if proxy_mode == "sticky":
            mode_label = "[green]sticky[/green]"
        elif proxy_mode == "round-robin":
            mode_label = "[cyan]round-robin[/cyan]"
        else:
            sp = next((p for p in providers if p["id"] == single_provider_id), None)
            sp_name = sp["name"] if sp else "?"
            mode_label = f"[yellow]single ({sp_name})[/yellow]"
        console.print(f"  [bold bright_cyan](1)[/bold bright_cyan] Port       [bold]{LOCAL_PORT}[/bold]")
        console.print(f"  [bold bright_green](2)[/bold bright_green] Mode       {mode_label}")
        console.print()
        console.print(f"  [dim]  sticky      — один провайдер, пока работает. При ошибке — следующий[/dim]")
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
                LOCAL_PORT = int(val)
                console.print("  [yellow]Restart proxy to apply.[/yellow]")
                press_any()
        elif ch == "2":
            modes = ["sticky", "round-robin", "single"]
            cur_idx = modes.index(proxy_mode) if proxy_mode in modes else 0
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
                    cur_sp = next((i for i, p in enumerate(active) if p["id"] == single_provider_id), 0)
                    sp_choice = select_option("Select provider", names, selected=cur_sp)
                    if sp_choice is None:
                        continue
                    single_provider_id = active[sp_choice]["id"]
                    db_set_setting("single_provider_id", str(single_provider_id))
                proxy_mode = new_mode
                db_set_setting("proxy_mode", proxy_mode)


# ─── Main ────────────────────────────────────────────────────
def main():
    global total_input_tokens, total_output_tokens, screen_dirty, proxy_mode, single_provider_id

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
        db_set_setting("proxy_mode", "sticky" if old_sticky == "1" else "round-robin")
    proxy_mode = db_get_setting("proxy_mode", "sticky")
    if proxy_mode not in ("sticky", "round-robin", "single"):
        proxy_mode = "sticky"
    single_provider_id = int(db_get_setting("single_provider_id", "0"))

    total_input_tokens, total_output_tokens = db_load_totals()
    refresh_today_tokens()

    threading.Thread(target=run_proxy_loop, daemon=True).start()
    time.sleep(0.3)

    while True:
        screen_main()
        ch = None
        while ch is None:
            if screen_dirty:
                screen_dirty = False
                screen_main()
            # Анимация ретраев — обновляем каждую секунду
            if active_retries:
                time.sleep(0.5)
                screen_main()
            if msvcrt.kbhit():
                c = msvcrt.getwch()
                if c in ("\x00", "\xe0"):
                    msvcrt.getwch()  # consume scan code of extended key
                elif c not in ("\r", "\n"):
                    ch = c
            else:
                time.sleep(0.1)
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


if __name__ == "__main__":
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
