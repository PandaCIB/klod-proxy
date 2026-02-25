# Klod Proxy

Local HTTP proxy for Anthropic Claude API (and compatible providers) with a built-in terminal UI.

Sits between your client (Claude Code, Cursor, etc.) and one or more API providers. Balances requests across providers, tracks token usage, and auto-retries on temporary failures — all from a single-file Python script.

![Windows](https://img.shields.io/badge/platform-Windows-blue)
![Linux](https://img.shields.io/badge/platform-Linux-blue)
![macOS](https://img.shields.io/badge/platform-macOS-blue)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-yellow)

## Features

- **Load balancing** — sticky, round-robin, or single-provider modes
- **Token tracking** — per-provider, per-model, and per-day usage statistics in SQLite
- **Auto-retry** — on `HTTP 500` with "no available accounts", retries across all providers with live counter in TUI
- **SSE streaming** — full support for streaming responses with real-time token counting
- **Cross-platform TUI** — manage providers, view stats, monitor logs on Windows, Linux, and macOS
- **Headless mode** — run on servers without TUI, with stdout logging and graceful shutdown
- **CLI management** — add, remove, toggle, and list providers from the command line
- **Yunyi API stats** — shows quota, usage, and expiration for yunyi.cfd providers

## Quick Start

```bash
git clone https://github.com/PandaCIB/klod-proxy.git
cd klod-proxy
pip install -r requirements.txt
python proxy.py
```

Windows — можно через установщик:

```bash
python install.py
klod
```

## Usage

1. Start the proxy — it listens on `http://localhost:8080`
2. Go to **Providers** (press `1`) and add your API endpoint (URL + API key)
3. Point your client to `http://localhost:8080` instead of the real API URL

### Claude Code

```bash
# Linux / macOS
export ANTHROPIC_BASE_URL=http://localhost:8080
export ANTHROPIC_API_KEY=klod

# Windows cmd
set ANTHROPIC_BASE_URL=http://localhost:8080
set ANTHROPIC_API_KEY=klod

# PowerShell
$env:ANTHROPIC_BASE_URL = "http://localhost:8080"
$env:ANTHROPIC_API_KEY = "klod"
```

API key может быть любым (`klod`) — прокси подставляет реальный ключ из активного провайдера.

### TUI

| Key | Action |
|-----|--------|
| `1` | Providers — add, edit, delete, toggle |
| `2` | Stats — tokens by model, provider, day + yunyi API quotas |
| `3` | Settings — port, mode (sticky/round-robin/single) |
| `0` | Clear log |

### CLI

```bash
python proxy.py add my-provider https://api.example.com/v1 sk-xxx
python proxy.py list
python proxy.py toggle 1
python proxy.py rm 1
python proxy.py --headless
```

## Headless / systemd

```ini
# /etc/systemd/system/klod-proxy.service
[Unit]
Description=Klod Proxy
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/klod-proxy
ExecStart=/usr/bin/python3 /opt/klod-proxy/proxy.py --headless
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now klod-proxy
journalctl -u klod-proxy -f
```

## How It Works

```
Client (Claude Code, Cursor, etc.)
        │
        ▼
   Klod Proxy (:8080)
        │
        ├──► Provider A (api.anthropic.com)
        ├──► Provider B (yunyi.cfd/claude)
        └──► Provider C (...)
```

Прокси перехватывает запросы, подставляет API-ключ выбранного провайдера, пробрасывает запрос, парсит токены из ответа (включая SSE-стримы) и логирует всё в локальную SQLite базу.

## Requirements

- Python 3.10+
- `aiohttp`, `rich`, `readchar`

## License

MIT
