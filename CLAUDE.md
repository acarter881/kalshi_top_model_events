# CLAUDE.md

## Project Overview

Monitors Kalshi prediction market events for AI model rankings (KXTOPMODEL, KXLLM1 series) and sends Discord webhook notifications when changes occur (new events, price movements, new options, settlements, settlement source changes, contract terms PDF updates).

## Architecture

Single-file Python script (`kalshi_monitor.py`) with no framework. Runs as a GitHub Actions cron job every 5 minutes (with a second cron offset by 2 minutes as a hedge against GitHub Actions cron delays), performing 10 checks per run with randomized 30-45s intervals (~5-7 min of continuous polling per run). A `kalshi-monitor` concurrency group with `cancel-in-progress: false` prevents overlapping runs from racing on the Actions cache. State is persisted via GitHub Actions cache (not committed to the repo). The Kalshi API is public and requires no authentication.

### Key Data Flow

1. Fetch open events from Kalshi API for each series in `SERIES_CONFIG`
2. Fetch event metadata (settlement sources) per event
3. Download and SHA-256 hash contract terms PDFs per series
4. Filter out yearly/year-end events
5. Diff new snapshot against cached state
6. Build Discord embeds for any changes
7. Save new state to `.github/state/kalshi_state.json`

## Running Locally

```bash
pip install -r requirements.txt  # just `requests`

# Dry run (no Discord messages, uses local state file)
python kalshi_monitor.py --dry-run

# Force send full summary
python kalshi_monitor.py --force-send --webhook-url "https://discord.com/api/webhooks/..."

# Loop mode (mimics CI)
python kalshi_monitor.py --loop --max-checks 10 --min-interval-seconds 30 --max-interval-seconds 45 --dry-run
```

State file defaults to `.github/state/kalshi_state.json` (gitignored). On first run with no state file, the script silently populates state without sending notifications (no old snapshot to diff against).

## Important Configuration

### SERIES_CONFIG (line ~38)
Dict keyed by series ticker (e.g., `KXTOPMODEL`). Each entry has:
- `slug` — URL path segment on kalshi.com
- `label` — human-readable name for Discord embeds
- `description` — not used in code, just documentation
- `contract_terms_url` — S3 URL for the contract terms PDF (hashed to detect rule changes)
- `sort_order` — controls ordering in Discord embeds (lower = first)

### State File
JSON at `.github/state/kalshi_state.json` containing:
- `snapshot` — full event/market data keyed by event ticker
- `pdf_hashes` — SHA-256 hashes of contract terms PDFs keyed by series ticker
- `last_check` / `last_changed` — ISO timestamps

In CI, state is persisted via `actions/cache` with key `kalshi-state-{branch}-{run_id}` and restore-key prefix matching. The state directory is gitignored.

### Discord Webhook
Set via `DISCORD_WEBHOOK_URL` env var or `--webhook-url` CLI arg. Stored as a GitHub Actions secret.

## Common Tasks

### Adding a new series
Add an entry to `SERIES_CONFIG` with the series ticker as key. Set `sort_order` to control embed ordering. After merging, do a manual `force_send` workflow run to seed the cache.

### Adjusting price threshold
Change `DEFAULT_PRICE_THRESHOLD` or pass `--price-threshold N` (in cents). The workflow YAML hardcodes some args — check both places.

### Changing check frequency
- Cron schedule: `.github/workflows/kalshi-monitor.yml` — there are two `cron:` entries (`*/5 * * * *` and a staggered `2-59/5 * * * *`); update both if you change cadence.
- Checks per run / intervals: workflow YAML `ARGS` line and/or CLI defaults in `parse_args()`. Per-run loop coverage should roughly match the cron interval to avoid dead air between runs.
- Concurrency: the `kalshi-monitor` concurrency group keeps overlapping runs serialized — required when increasing cadence so the Actions cache save/restore doesn't race.

## Key Functions

| Function | Role |
|----------|------|
| `fetch_series_events()` | Paginated fetch of open events with nested markets for a series |
| `fetch_event_metadata()` | Fetch settlement sources for a specific event |
| `fetch_pdf_hash()` | Download a PDF, return SHA-256 hex digest |
| `build_snapshot()` | Normalize raw API events into a diffable dict keyed by event ticker |
| `filter_yearly_events()` | Remove yearly/year-end events (matched by title keywords or `dec31` ticker suffix) |
| `derive_market_name()` | Extract human-readable model name from market fields, falling back to ticker suffix |
| `derive_event_period()` | Classify event as "Weekly" or "Monthly" from title/close-time heuristics |
| `diff_snapshots()` | Compare old vs new snapshot, return structured changes dict |
| `diff_pdf_hashes()` | Compare old vs new PDF hashes, return list of changed series |
| `build_embeds()` | Convert changes dict into batched Discord embed payloads (max 10 per message) |
| `run_single_check()` | Full cycle: load state, fetch, diff, notify, save state |

## Gotchas

- **First-run silent population**: When no state file exists, the first run saves state but sends no notifications (no old snapshot to diff against). This is intentional — prevents a flood of "new event" alerts on initial deploy or cache clear.

- **Yearly event filtering**: Events with "year", "annual", or "end of 202" in title/subtitle, or tickers ending in `dec31`, are silently excluded. If you need yearly events, modify `YEARLY_TITLE_KEYWORDS` and `is_yearly_event()`.

- **Price field is `yes_ask`, not `last_price`**: Diffs use `yes_ask` (current ask price) because `last_price` can be stale. This is documented in the diff function comment but easy to overlook.

- **Graceful degradation on fetch failure**: If fetching a series fails, old snapshot data for that series is carried forward to avoid false "removed event" alerts. Same pattern for settlement sources and PDF hashes.

- **Settlement source first-time skip**: `diff_snapshots` only alerts on settlement source changes if old sources existed (`if old_sorted != new_sorted and old_sources`). New events' sources are silently populated.

- **Discord rate limiting**: `_post_discord_payload` handles 429 responses with `retry_after`. Multi-message sends have a 1.5s delay between them.

- **`--force-send` overrides `--max-checks` to 1**: See `main()` line 1153.

- **State is not committed**: The `.github/state/` directory is gitignored. State lives only in GitHub Actions cache. Clearing the cache means starting fresh.

- **Workflow has `contents: read` only**: The workflow cannot push to the repo. State persistence is entirely via Actions cache.

## Branch/Workflow Conventions

- Single branch (`main`). The workflow runs on the default branch via cron.
- `workflow_dispatch` allows manual triggers with `force_send` and `dry_run` inputs.
- Python 3.12 in CI. Uses `|` union syntax for type hints (3.10+).
- Only dependency is `requests`.
