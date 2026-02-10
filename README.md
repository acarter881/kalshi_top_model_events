# kalshi_top_model_events

Monitors AI model prediction market events on [Kalshi](https://kalshi.com) and sends Discord notifications when changes are detected.

## Monitored Series

| Series | Label | Description | Example Events |
|--------|-------|-------------|----------------|
| `KXTOPMODEL` | Top AI Model | Which AI model will rank #1 on [LM Arena](https://lmarena.ai/) (weekly/monthly) | `KXTOPMODEL-26FEB14` (weekly), `KXTOPMODEL-26FEB28` (monthly) |
| `KXLLM1` | Best AI | Best AI model of the week/month/year | `KXLLM1-26FEB14` (weekly), `KXLLM1-26FEB28` (monthly) |

Each event contains multiple binary contracts — one per AI model (e.g., Gemini, ChatGPT, Claude, DeepSeek, Grok, Qwen). A contract priced at $0.57 implies a 57% probability that model wins.

## Notifications

Discord messages are sent when:

- **New event created** — a new weekly or monthly market appears (with top options and prices)
- **Price movement** — any contract moves by ≥5¢ (configurable via `--price-threshold`)
- **New option added** — a new AI model is added as a tradable option in an existing event
- **Market settled** — an event resolves with a winner
- **Event removed** — a market closes or disappears

## How It Works

```
GitHub Actions (cron every 10 min)
  └─ kalshi_monitor.py
       ├─ Fetches open events from Kalshi public API (no auth needed)
       │    GET /trade-api/v2/events?series_ticker=...&status=open&with_nested_markets=true
       ├─ Compares current snapshot against cached state
       ├─ Sends Discord webhook for any detected changes
       └─ Saves updated state to Actions cache
```

Each workflow run performs **4 checks** with randomized 60–120s intervals, giving roughly one check every ~1.5 minutes across each 10-minute window.

## Setup

1. **Create a Discord webhook** in your target channel:
   Server Settings → Integrations → Webhooks → New Webhook

2. **Add the webhook URL as a repository secret:**
   GitHub repo → Settings → Secrets and variables → Actions → New repository secret
   - Name: `DISCORD_WEBHOOK_URL`
   - Value: your webhook URL

3. **Enable the workflow** — it runs automatically via cron, or trigger manually:
   GitHub repo → Actions → Kalshi AI Model Monitor → Run workflow

## Manual Trigger Options

From the Actions tab, click "Run workflow" with optional inputs:

| Input | Description |
|-------|-------------|
| `force_send` | Send a full market summary even if nothing changed |
| `dry_run` | Run the monitor without actually sending Discord messages (logs only) |

## CLI Usage

```bash
# Single check, dry run
python kalshi_monitor.py --dry-run

# Force send current market summary
python kalshi_monitor.py --force-send

# Loop mode (what GitHub Actions uses)
python kalshi_monitor.py --loop --max-checks 4 --min-interval-seconds 60 --max-interval-seconds 120

# Custom price threshold (notify on ≥3¢ moves)
python kalshi_monitor.py --price-threshold 3
```

Run `python kalshi_monitor.py --help` for all options.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--price-threshold` | `5` | Minimum price change in cents to trigger a notification |
| `--max-checks` | `12` | Number of checks per loop run |
| `--min-interval-seconds` | `120` | Minimum sleep between checks |
| `--max-interval-seconds` | `300` | Maximum sleep between checks |
| `--retries` | `3` | Retry attempts for transient API/network failures |
| `--timeout-seconds` | `30` | HTTP request timeout |

To monitor additional Kalshi series, add entries to the `SERIES_CONFIG` dict in `kalshi_monitor.py`.
