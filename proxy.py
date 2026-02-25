import asyncio
import json
import msvcrt
import os
import sqlite3
import sys
import time
import threading
from aiohttp import web, ClientSession, ClientTimeout
from rich.console import Console
from rich.table import Table

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
    error_log.append(f"[{style}][{ts}] {msg}[/{style}]")
    if len(error_log) > MAX_ERRORS:
        error_log.pop(0)
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
    except Exception:
        pass


# ─── Провайдеры ──────────────────────────────────────────────
def get_active_providers() -> list[dict]:
    return [p for p in providers if p["active"]]


def next_provider() -> dict | None:
    global provider_index
    active = get_active_providers()
    if not active:
        return None
    provider_index = provider_index % len(active)
    p = active[provider_index]
    provider_index = (provider_index + 1) % len(active)
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
    path = request.path
    query = request.query_string

    headers = dict(request.headers)
    headers.pop("Host", None)
    for h in HOP_HEADERS:
        headers.pop(h, None)

    body = await request.read()
    method = request.method
    timeout = ClientTimeout(total=300, sock_read=300)

    while True:
        prov = next_provider()
        if not prov:
            log_error("No active providers configured", "yellow")
            return web.Response(status=503, text="No active providers configured")

        target_url = f"{prov['url']}{path}{'?' + query if query else ''}"
        headers["x-api-key"] = prov["key"]
        ctx = {"inp": 0, "out": 0, "model": ""}

        retry_count = 0
        retry_start = None

        async with ClientSession(timeout=timeout) as session:
            while True:
                async with session.request(method=method, url=target_url, headers=headers, data=body, ssl=True) as resp:
                    if resp.status >= 400:
                        err_body = await resp.read()
                        err_text = err_body.decode("utf-8", errors="replace")

                        if resp.status == 500 and "no avail" in err_text.lower():
                            if retry_count == 0:
                                retry_start = time.time()
                                log_error(f"{prov['name']}: No available accounts — retrying...", "yellow")
                            retry_count += 1
                            await asyncio.sleep(RETRY_DELAY)
                            continue

                        log_error(f"{resp.status} {prov['name']}: {err_text[:80]}")
                        return web.Response(
                            status=resp.status,
                            headers={k: v for k, v in resp.headers.items() if k not in STRIP_RESP_HEADERS},
                            body=err_body,
                        )

                    # Успех после ретраев
                    if retry_count > 0:
                        elapsed = int(time.time() - retry_start)
                        log_error(f"{prov['name']}: OK after {retry_count} retries ({elapsed}s)", "green")

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
                            db_save(ctx["model"], prov["id"], method, path, resp.status, ctx["inp"], ctx["out"])
                        return stream_resp
                    else:
                        resp_body = await resp.read()
                        try:
                            extract_tokens(json.loads(resp_body), ctx)
                        except (json.JSONDecodeError, ValueError):
                            pass
                        if ctx["inp"] or ctx["out"]:
                            db_save(ctx["model"], prov["id"], method, path, resp.status, ctx["inp"], ctx["out"])
                        return web.Response(status=resp.status, headers=resp_headers, body=resp_body)


# ─── Сервер ──────────────────────────────────────────────────
async def start_server():
    global runner
    app = web.Application()
    app.router.add_route("*", "/{path_info:.*}", proxy_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", LOCAL_PORT)
    await site.start()


async def stop_server():
    if runner:
        await runner.cleanup()


def run_proxy_loop():
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_server())
    loop.run_forever()


# ─── Ввод ────────────────────────────────────────────────────
def read_line(prompt: str = "") -> str:
    if prompt:
        console.print(prompt, end="")
    chars = []
    while True:
        if msvcrt.kbhit():
            ch = msvcrt.getwch()
            if ch in ("\r", "\n"):
                print()
                return "".join(chars).strip()
            elif ch == "\x08":
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


def read_key() -> str:
    while True:
        if msvcrt.kbhit():
            ch = msvcrt.getwch()
            if ch not in ("\r", "\n"):
                return ch
        time.sleep(0.05)


def press_any():
    while not msvcrt.kbhit():
        time.sleep(0.02)
    msvcrt.getwch()


def cls():
    # Перемещаем курсор в начало экрана без очистки — убирает моргание
    sys.stdout.write("\033[H\033[J")
    sys.stdout.flush()


# ─── TUI ─────────────────────────────────────────────────────
def fmt(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}m"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def bar(value: int, max_value: int, width: int = 20) -> str:
    if max_value == 0:
        return "[dim]" + "░" * width + "[/dim]"
    filled = int(value / max_value * width)
    return "[cyan]" + "█" * filled + "[/cyan][dim]" + "░" * (width - filled) + "[/dim]"


def screen_main():
    cls()
    refresh_today_tokens()
    status = "[bold green]Online[/bold green]" if loop and loop.is_running() else "[bold red]Offline[/bold red]"
    active = get_active_providers()
    today_total = today_input_tokens + today_output_tokens
    console.print()
    console.print(f"  [bold cyan]Status[/bold cyan]   {status}")
    console.print(f"  [dim]Listen[/dim]     http://localhost:{LOCAL_PORT}")
    console.print(f"  [dim]Providers[/dim]  {len(active)} active [dim]/ {len(providers)} total[/dim]")
    console.print(f"  [dim]Today[/dim]      [cyan]{fmt(today_input_tokens)}[/cyan] in  [green]{fmt(today_output_tokens)}[/green] out  [yellow]{fmt(today_total)}[/yellow] total")
    console.print()
    if error_log:
        console.print(f"  [dim]── Log ──[/dim]")
        for line in error_log[-10:]:
            console.print(f"  {line}")
        console.print()
    console.print(f"  [dim]1[/dim] Providers   [dim]2[/dim] Stats   [dim]3[/dim] Settings   [dim]0[/dim] Clear log")
    console.print()


def screen_providers():
    while True:
        cls()
        reload_providers()
        console.print()
        console.print(f"  [bold cyan]Providers[/bold cyan]")
        console.print()
        if providers:
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
                t_inp, t_out = db_load_totals(p["id"])
                table.add_row(str(p["id"]), p["name"], p["url"], masked, st, fmt(t_inp + t_out))
            console.print(table)
        else:
            console.print("  [dim]No providers. Add one to start proxying.[/dim]")
        console.print()
        console.print(f"  [dim]a[/dim] Add   [dim]d[/dim] Delete   [dim]t[/dim] Toggle on/off   [dim]0[/dim] Back")
        console.print()
        ch = read_line("  [dim]>[/dim] ")
        if ch == "a":
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
        elif ch == "d":
            pid = read_line("  Provider # to delete: ")
            if pid.isdigit():
                db_remove_provider(int(pid))
                reload_providers()
                console.print("  [red]Deleted.[/red]")
                press_any()
        elif ch == "t":
            pid = read_line("  Provider # to toggle: ")
            if pid.isdigit():
                db_toggle_provider(int(pid))
                reload_providers()
        elif ch == "0":
            break


def screen_stats():
    cls()
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

    console.print()
    console.print("  [dim]Press any key...[/dim]")
    press_any()


def screen_settings():
    global LOCAL_PORT
    while True:
        cls()
        console.print()
        console.print(f"  [bold yellow]Settings[/bold yellow]")
        console.print()
        console.print(f"  [dim]1[/dim] Port       [bold]{LOCAL_PORT}[/bold]")
        console.print(f"  [dim]0[/dim] Back")
        console.print()
        ch = read_line("  [dim]>[/dim] ")
        if ch == "1":
            val = read_line("  New port: ")
            if val.isdigit():
                LOCAL_PORT = int(val)
                console.print("  [yellow]Restart proxy to apply.[/yellow]")
                press_any()
        elif ch == "0":
            break


# ─── Main ────────────────────────────────────────────────────
def main():
    global total_input_tokens, total_output_tokens, screen_dirty

    # Включаем ANSI escape codes на Windows
    if sys.platform == "win32":
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)

    init_db()
    reload_providers()

    total_input_tokens, total_output_tokens = db_load_totals()
    refresh_today_tokens()

    threading.Thread(target=run_proxy_loop, daemon=True).start()
    time.sleep(0.3)

    while True:
        screen_main()
        ch = None
        last_slot = int(time.time()) // 300
        while ch is None:
            if screen_dirty:
                screen_dirty = False
                screen_main()
            # Перерисовка на границе 5-минутного слота
            cur_slot = int(time.time()) // 300
            if cur_slot != last_slot:
                last_slot = cur_slot
                screen_main()
            if msvcrt.kbhit():
                c = msvcrt.getwch()
                if c not in ("\r", "\n"):
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
            error_log.clear()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n  [red]Interrupted.[/red]")
        if loop:
            asyncio.run_coroutine_threadsafe(stop_server(), loop)
            loop.call_soon_threadsafe(loop.stop)
