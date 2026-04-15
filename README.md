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
- **Settlement source changed** — Kalshi changes the data source used to resolve a market (e.g., swaps one leaderboard URL for another)
- **Contract rules updated** — the contract terms PDF for a series is modified (rule changes, tiebreaker updates, eligibility changes, etc.)

## How It Works

```
GitHub Actions (cron every 5 min, plus a 2-minute-staggered backup schedule)
  └─ kalshi_monitor.py
       ├─ Fetches open events from Kalshi public API (no auth needed)
       │    GET /trade-api/v2/events?series_ticker=...&status=open&with_nested_markets=true
       ├─ Fetches event metadata for settlement sources
       │    GET /trade-api/v2/events/{event_ticker}/metadata
       ├─ Downloads & hashes contract terms PDFs to detect rule changes
       │    https://kalshi-public-docs.s3.amazonaws.com/contract_terms/{SERIES}.pdf
       ├─ Compares current snapshot against cached state
       ├─ Sends Discord webhook for any detected changes
       └─ Saves updated state to Actions cache
```

Each workflow run performs **10 checks** with randomized 30–45s intervals (~5–7 min of continuous polling), and the cron fires every 5 minutes. Two staggered cron entries (`*/5` and `2-59/5`) hedge against GitHub Actions' best-effort cron delays, while a `kalshi-monitor` concurrency group prevents overlapping runs from racing on the Actions cache.

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

### When to manually run the action

You **should** trigger a manual run with `force_send` after:

- **Merging code changes** that affect state structure (new fields in the snapshot, new series added, filter changes). The first run after such changes seeds the new data into the cache so subsequent automated runs can diff against it correctly. Without this, the first automated run may produce a false positive or silently skip alerting on new data categories.
- **Clearing the Actions cache** — if you delete the `kalshi-state-*` cache entry, the next run starts from a blank slate. A `force_send` run will both repopulate the cache and send you a full market summary so you can confirm everything looks right.

You **do not** need to manually run after routine changes (e.g., tweaking the price threshold or cron schedule) — the next scheduled run will pick those up automatically.

## CLI Usage

```bash
# Single check, dry run
python kalshi_monitor.py --dry-run

# Force send current market summary
python kalshi_monitor.py --force-send

# Loop mode (what GitHub Actions uses)
python kalshi_monitor.py --loop --max-checks 10 --min-interval-seconds 30 --max-interval-seconds 45

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
