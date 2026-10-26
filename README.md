# solace-subber

Python Solace debugging tools using the official [`solace-pubsubplus`](https://pypi.org/project/solace-pubsubplus/) API.

Included tools:

- **`consumer.py`** — consumes messages from a **topic** or **queue** over plain or secure SMF, with continuous or step mode, concise/full console output, and optional file logging (`full` NDJSON, `simple`, or `payload_only` lines).
- **`browser.py`** — non-destructive **queue browser** with oldest-first pagination, compact one-line items, and on-demand full dumps.
- **`publisher.py`** — publishes to a **topic** or **directly to a queue**. Interactive menu to send payloads, replay consumer NDJSON lines (`sr`), burst-send (`s<n>`), resend last, and change destination.
- **`xfer.py`** — siphon a file (max 100 MiB) over SMF as META / DATA / END messages. Sender and receiver share a queue (provisioned if needed) or a topic (Direct; receiver must already be subscribed).

Planned work (not implemented yet): see [BACKLOG.md](./BACKLOG.md) — next item is a **replayer** (consumer-like siphon to a sink topic/queue, with browse vs drain).

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

Copy [`.env.example`](./.env.example) to `.env` in your **current working directory** (or beside the scripts). Environment variables are used as fallback. Pass `-e` / `--env` to load a specific file: a path (`-e .env.dev`, `-e path/to/file`) or a short name (`-e dev` → `.env.dev`). Relative paths resolve against the current working directory; a missing file is an error. If `-e`/`--env` is omitted and no `.env` is found, a warning is printed and the process environment is used.

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
| `SOLACE_SOURCE_TYPE` | `topic` or `queue` (consume source / publisher default dest type) | `topic` |
| `SOLACE_TOPIC` | Topic(s) for consume (comma-separated list OK) / publisher default topic | — |
| `SOLACE_QUEUE` | Queue name when `queue`, browser default, or publisher queue dest | — |
| `SOLACE_BROWSER_PAGE_SIZE` | Default browser page size | `10` |
| `SOLACE_CONSUME_MODE` | `continuous` or `step`. When **unset**, defaults by dest: queue→`step`, topic→`continuous` | *(auto)* |
| `SOLACE_RECONNECT_MODE` | `seamless` (keep consuming / rebind) or `stop` (return to mode menu) | `seamless` |
| `SOLACE_DELIVERY_MODE` | Publisher: `auto` / `direct` / `persistent` (`auto` = Direct for topic, Persistent for queue) | `auto` |
| `SOLACE_CONSOLE_FULL` | Full metadata + payload on console | `false` |
| `SOLACE_LOG_TO_FILE` | Append events to a log file | `true` |
| `SOLACE_LOG_FILE` | Log path (relative to cwd) | `solace-events.log` |
| `SOLACE_LOG_FORMAT` | Log line format: `full`/`ndjson` (NDJSON), `simple`/`compact`, or `payload_only` (`payload`/`payload-only`/`body`) | `full` |
| `SOLACE_LOG_WARN_MB` | Warn + confirm at startup if existing log is ≥ this many MiB (`0` = off) | `50` |
| `SOLACE_LOG_WARN_BYTES` | Optional exact byte threshold (overrides `SOLACE_LOG_WARN_MB`) | — |
| `SOLACE_XFER_CHUNK_BYTES` | `xfer.py` DATA chunk size (bytes). Max `10000000` (Solace common max payload). Prompt can override. | `1048576` (1 MiB) |

## Run

### Consumer

```bash
python consumer.py
python consumer.py -e dev                 # loads .env.dev
python consumer.py -e ./configs/broker.env
python consumer.py --env /abs/path/to/.env
```

Interactive menu loop (same UX as `solace-consumer` in solace-qdeb):

1. Dest type: `[q]ueue` / `[t]opic` (default from env)
2. Queue/topic name prompt (fixed for the session). Topic accepts a comma-separated list (e.g. `a/b, c/d/>`); each entry is subscribed.
3. Mode: `[c]ontinuous` / `[s]tep` / `[q]uit` — default from `SOLACE_CONSUME_MODE`, or queue→step / topic→continuous when unset
4. Start gate — press Enter to begin (prints a one-liner consume summary)
5. **Ctrl+C** stops consuming and returns to the menu (does not exit)
6. **Ctrl+C** or `q` at the menu quits

`SOLACE_RECONNECT_MODE=seamless` (default) rebinds all topic subscriptions (or the queue receiver) after reconnect and reprints the summary; `stop` returns to the mode menu. Large existing log files (≥ `SOLACE_LOG_WARN_MB`, default 50) prompt before connect.

When `SOLACE_LOG_TO_FILE=true` (default), every consumed message is appended to `SOLACE_LOG_FILE`. `SOLACE_LOG_FORMAT` selects the line format (default **`full`**):

- **`full`** (alias `ndjson`) — NDJSON: one JSON object per line, metadata + full payload.
- **`simple`** (alias `compact`) — `[{timestamp}]::[{topic}]::{payload}`. Timestamp is broker sender/receiver UTC when available (same fallback as the console); otherwise local receive time with offset. Newlines/CRs in the payload are escaped as `\n` / `\r` (one physical line, no truncation).
- **`payload_only`** (aliases `payload`, `payload-only`, `body`) — each line is only the UTF-8 payload (untruncated). Newlines/CRs are escaped as `\n` / `\r`. Empty payload writes a blank line. No timestamp, topic, JSON wrapper, or other metadata.

Step mode: queue messages are acked only after display; topic step drops messages that arrive while paused (bounded buffer of 1).

### Browser

```bash
python browser.py
python browser.py -e dev      # loads .env.dev
python browser.py -e ./configs/broker.env
```

Prompts for queue name and page size, then fetches page 1 (oldest messages — no full count scan). **Item #1 is oldest**; page 1 is the head of the queue. Commands: page number, `n`/`p`, `s`/`size`, `r`, `i<n>`, `d` (current page dump), `a`/`a<n>` (dump all oldest-first to the log file, optional limit), `h`, `q`. **Ctrl+C** quits (during `a`, aborts the dump and returns to the menu). Until the last page is reached, the status line shows `items 1-10 of ?` / `page 2+`.

### Publisher

```bash
python publisher.py
python publisher.py -e dev      # loads .env.dev
python publisher.py -e ./configs/broker.env
```

Prompts for topic or queue (from `SOLACE_SOURCE_TYPE`), validates queue destinations via a short browse bind, then enters the publish menu:

| Command | Action |
| --- | --- |
| `s` | Prompt for a payload line; send UTF-8 bytes as-is |
| `sr` | Paste one consumer NDJSON/JSON line; rebuild & send |
| `s<n>` | Prompt once, send that payload *n* times (e.g. `s5`) |
| `r` | Resend last payload (WARN if nothing sent yet) |
| `cd` | Change destination type (topic/queue) and name |
| `h` | Help (pause before redraw) |
| `q` | Quit and print session stats |

**Ctrl+C** at the menu or destination prompt quits cleanly.

Delivery mode from `SOLACE_DELIVERY_MODE`: **`auto`** (Direct for topic, Persistent for queue), or force **`direct`** / **`persistent`**. Persistent sends wait for broker ack.

`sr` restores payload and common headers when present; broker-only fields (`adMessageId`, `receiverTimestamp`, `redelivered`, etc.) are WARNed and skipped.

### File transfer (`xfer.py`)

```bash
python xfer.py
python xfer.py -e dev
python xfer.py -e dev ./file.bin   # sender file (role still asked)
```

Same `.env` connection keys as the other tools (`SOLACE_HOST`, VPN, credentials, TLS). No event log file. Chunk size from `SOLACE_XFER_CHUNK_BYTES` (default 1 MiB) and/or a prompt; values **> 10_000_000** are rejected (Solace common max payload). Files larger than **100 MiB** are rejected.

**Envelope** (user properties + binary attachment):

| Message | `xfer-kind` | Attachment | Other user props |
| --- | --- | --- | --- |
| **META** (must be first) | `meta` | JSON UTF-8: `kind`, `transferId`, `fileName`, `fileSize`, `chunkSize`, `totalChunks`, `sha256` (hex), optional `contentType` | `xfer-id` |
| **DATA** | `data` | raw file bytes | `xfer-id`, `xfer-seq` (0-based) |
| **END** | `end` | JSON UTF-8: `kind`, `transferId`, `sha256` | `xfer-id` |

1. **Role?** `[s]ender` / `[r]eceiver`
2. **Dest type?** `[q]ueue` / `[t]opic` (default **queue**)
3. **Name?** Empty queue → `xfer-{8 hex}`; empty topic → `xfer/{guid}` (printed so you can copy it to the other side)

**Queue provision:** both roles try CCSMP endpoint provision of a **durable exclusive** queue. If it is created here, `createdByUs=true` and this side owns the delete prompt. If it already exists, it is reused (`createdByUs=false`, no delete). If provision is denied and the queue is missing: **sender** is offered a **topic** fallback (default random, copyable); **receiver** errors (cannot receive from a missing queue). Existing queues are bound even when create is not allowed.

**Delivery:** Persistent to a queue (published to reserved topic `#P2P/QUE/<queueName>` — the Python API accepts Topic destinations only). Direct to a topic, with a WARN that the receiver must already be subscribed.

**Sender:** path (CLI positional after `-e`, else prompt) → SHA-256 → confirm receiver started → META, DATA chunks, END → summary. If this side created the queue and the send **succeeded**, wait up to 30s (poll every 2s via non-destructive browse) until the queue is empty, then `Delete queue? [y]/n` (default y). On **error or Ctrl+C**, skip the empty-wait (queue may still have messages) and still prompt delete immediately.

**Receiver:** peek the first message with a queue browser (does not consume). If it is not META, fail and leave the queue untouched. If META is OK, prompt the save path (default from META `fileName`; will not overwrite — asks to use `<name>-N.ext`). Then bind a ClientAck receiver, consume and ack META, then DATA in order, then END; verify SHA-256. If browsing is unavailable, the fallback is: consume META with ClientAck, **hold it unacked** until the filename is confirmed, then Ack and continue. **Never NACK/Reject** (that can DLQ). Out-of-order or wrong `transferId` fails without acking the bad message.

On **success, error, or Ctrl+C**: if `createdByUs`, the receiver prompts `Delete queue? [y]/n` immediately (no 30s wait). If this side did **not** create the queue, there is no delete prompt. A second Ctrl+C during the delete prompt skips delete and prints the queue name.

`.NET` and Python xfer envelopes are interoperable (same user properties and JSON).

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
python publisher.py
python xfer.py
```

On Windows PowerShell, use `${PWD}` instead of `$(pwd)`.

## Project layout

```
solace-subber/
  common/           shared: CLI (-e/--env), .env, config, connection, payload format, file logger, browser
  consumer.py       topic/queue consumer
  browser.py        queue browser
  publisher.py      topic/queue publisher
  xfer.py           file transfer (META/DATA/END)
  requirements.txt
  Dockerfile
  .env.example
```

## Differences from solace-qdeb (.NET)

| Area | .NET (`solace-qdeb`) | Python (`solace-subber`) |
| --- | --- | --- |
| Distribution | Single self-contained binary | `python consumer.py` / `browser.py` / `publisher.py` / `xfer.py` + `pip install` |
| API | `SolaceSystems.Solclient.Messaging` | `solace-pubsubplus` |
| Full metadata dump | `Message.Dump(Full)` | `str(message)` plus explicit getters (no identical dump API) |
| AD message id | `ADMessageId` | `get_sequence_number()` when available |
| Queue bind | `IFlow` with explicit window size | `PersistentMessageReceiver` with client ack; step mode uses sync `receive_message()` |
| Direct-to-queue publish | `IQueue` destination on `ISession.Send` | Persistent publish to reserved topic `#P2P/QUE/<queueName>` (Python API accepts Topic destinations only) |
| Queue provision/delete (`xfer`) | `ISession.Provision` / `Deprovision` | CCSMP `endpointProvision` / `endpointDeprovision` (no public Python wrapper) |
| .env search order | Beside binary, then cwd; `-e`/`--env` path or short name (`dev` → `.env.dev`) | **cwd first**, then beside scripts; `-e`/`--env` path or short name (`dev` → `.env.dev`) |

Behavior and prompts are intentionally aligned with the .NET tools where the Python API allows.

### Timestamps

Solace sender/receiver timestamps from the broker are **UTC epoch milliseconds**. Console
and NDJSON output show them as UTC (e.g. `2026-07-09 17:30:45.123 UTC`). The local
`received_at (local)` field / `receivedAt` NDJSON key (ISO-8601 with offset) is when
the tool processed the message.
