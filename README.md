# solace-subber

Python Solace debugging tools using the official [`solace-pubsubplus`](https://pypi.org/project/solace-pubsubplus/) API.

Included tools:

- **`consumer.py`** — consumes messages from a **topic** or **queue** over plain or secure SMF, with continuous or step mode, concise/full console output, and optional NDJSON file logging.
- **`browser.py`** — non-destructive **queue browser** with tail-first pagination, compact one-line items, and on-demand full dumps.

## Requirements

- Python 3.10+ (3.12 recommended)
- Network access to your Solace broker

## Setup

```bash
cd solace-subber
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env   # edit with your broker settings
```

## Configuration

Copy [`.env.example`](./.env.example) to `.env` in your **current working directory** (or beside the scripts). Environment variables are used as fallback.

| Key | Description | Default |
| --- | --- | --- |
| `SOLACE_HOST` | `host:port` or full URL (`tcp://` / `tcps://`) | *(required)* |
| `SOLACE_VPN` | Message VPN | `default` |
| `SOLACE_USERNAME` | Client username | *(required)* |
| `SOLACE_PASSWORD` | Client password | *(empty)* |
| `SOLACE_SECURE` | `true` => `tcps://`, `false` => `tcp://` | `false` |
| `SOLACE_SSL_VALIDATE_CERT` | Validate TLS cert when secure | `false` |
| `SOLACE_CONNECT_TIMEOUT_MS` | Connect timeout (ms) | `10000` |
| `SOLACE_RECONNECT_RETRIES` | Reconnect attempts | `3` |
| `SOLACE_SOURCE_TYPE` | `topic` or `queue` (consumer) | `topic` |
| `SOLACE_TOPIC` | Topic subscription when `topic` | — |
| `SOLACE_QUEUE` | Queue name when `queue`, or browser default | — |
| `SOLACE_BROWSER_PAGE_SIZE` | Default browser page size | `10` |
| `SOLACE_CONSUME_MODE` | `continuous` or `step` | `step` |
| `SOLACE_CONSOLE_FULL` | Full metadata + payload on console | `false` |
| `SOLACE_LOG_TO_FILE` | Append NDJSON to log file | `true` |
| `SOLACE_LOG_FILE` | Log path (relative to cwd) | `solace-events.log` |

## Run

### Consumer

```bash
python consumer.py
```

Interactive menu loop (same UX as `solace-consumer` in solace-qdeb):

1. Queue/topic prompt (fixed for the session)
2. Mode: `[c]ontinuous` / `[s]tep` / `[q]uit`
3. Start gate — press Enter to begin
4. **Ctrl+C** stops consuming and returns to the menu (does not exit)
5. **Ctrl+C** or `q` at the menu quits

Step mode: queue messages are acked only after display; topic step drops messages that arrive while paused (bounded buffer of 1).

### Browser

```bash
python browser.py
```

Prompts for queue name and page size, scans the queue (oldest-first count), then browses interactively. **Item #1 is newest.** Commands: page number, `n`/`p`, `s`/`size`, `r`, `i<n>`, `d` (console + log), `h`, `q`. **Ctrl+C** quits.

## Docker

Build an interactive image (drops you into a shell; run tools manually):

```bash
docker build -t solace-subber .
```

Run with your `.env` mounted and an interactive TTY:

```bash
docker run --rm -it \
  -v "$(pwd)/.env:/app/.env:ro" \
  -v "$(pwd)/solace-events.log:/app/solace-events.log" \
  solace-subber

# inside the container:
python consumer.py
python browser.py
```

On Windows PowerShell, use `${PWD}` instead of `$(pwd)`.

## Project layout

```
solace-subber/
  common/           shared: .env, config, connection, payload format, file logger, browser
  consumer.py       topic/queue consumer
  browser.py        queue browser
  requirements.txt
  Dockerfile
  .env.example
```

## Differences from solace-qdeb (.NET)

| Area | .NET (`solace-qdeb`) | Python (`solace-subber`) |
| --- | --- | --- |
| Distribution | Single self-contained binary | `python consumer.py` / `browser.py` + `pip install` |
| API | `SolaceSystems.Solclient.Messaging` | `solace-pubsubplus` |
| Full metadata dump | `Message.Dump(Full)` | `str(message)` plus explicit getters (no identical dump API) |
| AD message id | `ADMessageId` | `get_sequence_number()` when available |
| Queue bind | `IFlow` with explicit window size | `PersistentMessageReceiver` with client ack; step mode uses sync `receive_message()` |
| .env search order | Beside binary, then cwd | **cwd first**, then beside scripts |

Behavior and prompts are intentionally aligned with the .NET tools where the Python API allows.

### Timestamps

Solace sender/receiver timestamps from the broker are **UTC epoch milliseconds**. Console
and NDJSON output show them as UTC (e.g. `2026-07-09 17:30:45.123 UTC`). The local
`received_at (local)` field / `receivedAt` NDJSON key (ISO-8601 with offset) is when
the tool processed the message.
