"""
Kalshi AI Model Event Monitor

Monitors Kalshi prediction market events for AI model rankings and sends
Discord notifications when significant changes are detected, including:
- New events created (e.g., new weekly/monthly market)
- Significant price movements on existing contracts
- New market options added to existing events
- Markets settled/resolved
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WEB_BASE = "https://kalshi.com/markets"

# Series we monitor
SERIES_CONFIG = {
    "KXTOPMODEL": {
        "slug": "top-model",
        "label": "Top AI Model",
    },
    "KXLLM1": {
        "slug": "yearend-top-llm",
        "label": "Best AI",
    },
}

DEFAULT_STATE_FILE = ".github/state/kalshi_state.json"
DEFAULT_PRICE_THRESHOLD = 5  # cents
REQUEST_TIMEOUT = 30

# Keywords in event titles that indicate a yearly event (case-insensitive)
YEARLY_TITLE_KEYWORDS = ("year", "annual", "end of 202")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor Kalshi AI model events")
    parser.add_argument(
        "--webhook-url",
        default=os.environ.get("DISCORD_WEBHOOK_URL", ""),
        help="Discord webhook URL (default: DISCORD_WEBHOOK_URL env var)",
    )
    parser.add_argument(
        "--state-file",
        default=DEFAULT_STATE_FILE,
        help=f"Path to state file (default: {DEFAULT_STATE_FILE})",
    )
    parser.add_argument(
        "--price-threshold",
        type=int,
        default=DEFAULT_PRICE_THRESHOLD,
        help=f"Price change threshold in cents to trigger notification (default: {DEFAULT_PRICE_THRESHOLD})",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run in loop mode with multiple checks",
    )
    parser.add_argument(
        "--min-interval-seconds",
        type=int,
        default=120,
        help="Minimum interval between checks in seconds (default: 120)",
    )
    parser.add_argument(
        "--max-interval-seconds",
        type=int,
        default=300,
        help="Maximum interval between checks in seconds (default: 300)",
    )
    parser.add_argument(
        "--max-checks",
        type=int,
        default=12,
        help="Maximum number of checks per run (default: 12)",
    )
    parser.add_argument(
        "--force-send",
        action="store_true",
        help="Send notification even if no changes detected",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without sending Discord messages",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=REQUEST_TIMEOUT,
        help=f"HTTP request timeout in seconds (default: {REQUEST_TIMEOUT})",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retry attempts for transient failures (default: 3)",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=2.0,
        help="Base backoff multiplier for retries in seconds (default: 2.0)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def fetch_with_retries(
    url: str,
    *,
    params: dict | None = None,
    timeout: int = REQUEST_TIMEOUT,
    retries: int = 3,
    backoff: float = 2.0,
) -> dict:
    """Fetch JSON from a URL with exponential backoff retries."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "KalshiMonitor/1.0",
    }

    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = requests.get(
                url, params=params, headers=headers, timeout=timeout
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            if resp.status_code < 500:
                # Client error — don't retry
                logger.error("HTTP %s for %s: %s", resp.status_code, url, resp.text)
                raise
            last_exc = exc
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc

        if attempt < retries:
            wait = backoff * (2**attempt) + random.uniform(0, 1)
            logger.warning(
                "Request to %s failed (attempt %d/%d), retrying in %.1fs: %s",
                url,
                attempt + 1,
                retries + 1,
                wait,
                last_exc,
            )
            time.sleep(wait)

    raise RuntimeError(f"All {retries + 1} attempts to {url} failed") from last_exc


# ---------------------------------------------------------------------------
# Kalshi API
# ---------------------------------------------------------------------------


def fetch_series_events(
    series_ticker: str, *, timeout: int = REQUEST_TIMEOUT, retries: int = 3, backoff: float = 2.0
) -> list[dict]:
    """Fetch all open events for a series, with nested markets."""
    url = f"{KALSHI_API_BASE}/events"
    all_events: list[dict] = []
    cursor: str | None = None

    while True:
        params: dict[str, Any] = {
            "series_ticker": series_ticker,
            "status": "open",
            "with_nested_markets": "true",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor

        data = fetch_with_retries(
            url, params=params, timeout=timeout, retries=retries, backoff=backoff
        )

        events = data.get("events") or []
        all_events.extend(events)

        cursor = data.get("cursor")
        if not cursor:
            break

    return all_events


def derive_market_name(market: dict) -> str:
    """
    Derive a human-readable name for a market option.

    Tries multiple fields in order of preference, falling back to
    extracting the suffix from the market ticker itself.
    """
    for field in ("subtitle", "yes_sub_title", "no_sub_title"):
        val = (market.get(field) or "").strip()
        if val:
            return val

    # Fall back to ticker suffix: e.g. KXTOPMODEL-26FEB14-GEMINI -> GEMINI
    ticker = market.get("ticker", "")
    parts = ticker.split("-")
    if len(parts) >= 3:
        suffix = "-".join(parts[2:])
        # Titlecase it for readability: CHATGPT -> Chatgpt, GPT4O -> Gpt4O
        return suffix.replace("-", " ").title()

    # Last resort
    return market.get("title", ticker) or ticker


def build_snapshot(events: list[dict]) -> dict:
    """
    Build a normalized snapshot from raw Kalshi events.

    Returns a dict keyed by event_ticker, each containing:
      - event metadata (title, dates)
      - markets dict keyed by market ticker with prices / volume
    """
    snapshot: dict[str, Any] = {}
    for event in events:
        event_ticker = event.get("event_ticker", "")
        markets_raw = event.get("markets") or []

        markets: dict[str, Any] = {}
        for m in markets_raw:
            ticker = m.get("ticker", "")
            name = derive_market_name(m)
            markets[ticker] = {
                "name": name,
                "title": m.get("title", ""),
                "subtitle": m.get("subtitle", ""),
                "yes_sub_title": m.get("yes_sub_title", ""),
                "status": m.get("status", ""),
                "yes_bid": m.get("yes_bid"),
                "yes_ask": m.get("yes_ask"),
                "last_price": m.get("last_price"),
                "volume": m.get("volume"),
                "volume_24h": m.get("volume_24h"),
                "open_interest": m.get("open_interest"),
                "close_time": m.get("close_time", ""),
                "result": m.get("result", ""),
            }

        snapshot[event_ticker] = {
            "title": event.get("title", ""),
            "sub_title": event.get("sub_title", ""),
            "series_ticker": event.get("series_ticker", ""),
            "strike_date": event.get("strike_date", ""),
            "category": event.get("category", ""),
            "markets": markets,
        }

    return snapshot


def is_yearly_event(event: dict) -> bool:
    """Check if an event is a yearly/year-end event that should be excluded."""
    title = (event.get("title") or "").lower()
    sub_title = (event.get("sub_title") or "").lower()
    for kw in YEARLY_TITLE_KEYWORDS:
        if kw in title or kw in sub_title:
            return True
    # Ticker ending in dec31 is a year-end event (e.g. KXLLM1-26DEC31)
    ticker = (event.get("event_ticker") or "").lower()
    if ticker.endswith("dec31"):
        return True
    return False


def filter_yearly_events(snapshot: dict) -> dict:
    """Remove yearly/year-end events from a snapshot."""
    return {
        et: ev for et, ev in snapshot.items() if not is_yearly_event(ev)
    }


def event_sort_key(event_ticker: str, snapshot: dict) -> str:
    """
    Return a sort key that orders events by close time ascending.

    Weekly events close sooner than monthly, so this naturally
    produces weekly-first ordering.
    """
    ev = snapshot.get(event_ticker, {})
    markets = ev.get("markets", {})
    if markets:
        first_market = next(iter(markets.values()))
        close_time = first_market.get("close_time", "")
        if close_time:
            return close_time
    # Fall back to strike_date, then ticker (alphabetical)
    return ev.get("strike_date", "") or event_ticker


# ---------------------------------------------------------------------------
# Diffing
# ---------------------------------------------------------------------------


def diff_snapshots(
    old: dict, new: dict, price_threshold: int
) -> dict[str, Any]:
    """
    Compare two snapshots and return structured changes.

    Returns dict with keys:
      new_events       – list of event tickers that are brand new
      removed_events   – list of event tickers that disappeared
      new_markets      – list of (event_ticker, market_ticker) for new options
      removed_markets  – list of (event_ticker, market_ticker)
      price_changes    – list of dicts with price movement details
      settled_markets  – list of (event_ticker, market_ticker, result)
    """
    changes: dict[str, list] = {
        "new_events": [],
        "removed_events": [],
        "new_markets": [],
        "removed_markets": [],
        "price_changes": [],
        "settled_markets": [],
    }

    old_event_tickers = set(old.keys())
    new_event_tickers = set(new.keys())

    # New events
    for et in sorted(new_event_tickers - old_event_tickers):
        changes["new_events"].append(et)

    # Removed events
    for et in sorted(old_event_tickers - new_event_tickers):
        changes["removed_events"].append(et)

    # Compare markets within shared events
    for et in sorted(old_event_tickers & new_event_tickers):
        old_markets = old[et].get("markets", {})
        new_markets = new[et].get("markets", {})

        old_mt = set(old_markets.keys())
        new_mt = set(new_markets.keys())

        for mt in sorted(new_mt - old_mt):
            changes["new_markets"].append((et, mt))

        for mt in sorted(old_mt - new_mt):
            changes["removed_markets"].append((et, mt))

        # Price changes on shared markets
        for mt in sorted(old_mt & new_mt):
            om = old_markets[mt]
            nm = new_markets[mt]

            old_price = om.get("last_price")
            new_price = nm.get("last_price")

            # Detect settlement
            if om.get("status") != "settled" and nm.get("status") == "settled":
                changes["settled_markets"].append((et, mt, nm.get("name", mt)))
                continue

            if old_price is not None and new_price is not None:
                delta = new_price - old_price
                if abs(delta) >= price_threshold:
                    changes["price_changes"].append(
                        {
                            "event_ticker": et,
                            "market_ticker": mt,
                            "name": nm.get("name", mt),
                            "old_price": old_price,
                            "new_price": new_price,
                            "delta": delta,
                            "volume_24h": nm.get("volume_24h"),
                        }
                    )

    return changes


def has_changes(changes: dict) -> bool:
    return any(bool(v) for v in changes.values())


# ---------------------------------------------------------------------------
# Discord messaging
# ---------------------------------------------------------------------------


def validate_webhook_url(url: str) -> bool:
    if not url:
        return False
    return url.startswith("https://discord.com/api/webhooks/") or url.startswith(
        "https://discordapp.com/api/webhooks/"
    )


def cents_str(cents: int | None) -> str:
    """Format a cent price as a readable string: 57¢ (57%)."""
    if cents is None:
        return "—"
    return f"{cents}¢ ({cents}%)"


def event_web_url(series_ticker: str, event_ticker: str) -> str:
    """Build a Kalshi web URL for an event."""
    cfg = SERIES_CONFIG.get(series_ticker, {})
    slug = cfg.get("slug", series_ticker.lower())
    return f"{KALSHI_WEB_BASE}/{series_ticker.lower()}/{slug}/{event_ticker.lower()}"


def format_close_time(close_time: str) -> str:
    """Format an ISO close time to a short human string."""
    if not close_time:
        return ""
    try:
        dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %Y %H:%M UTC")
    except (ValueError, TypeError):
        return close_time


# Discord embed color palette
COLOR_NEW_EVENT = 0x57F287  # green
COLOR_PRICE_MOVE = 0xFEE75C  # yellow
COLOR_NEW_OPTION = 0x5865F2  # blurple
COLOR_SETTLED = 0xEB459E  # pink/red
COLOR_REMOVED = 0xED4245  # red
COLOR_SUMMARY = 0x5865F2  # blurple


def build_market_table(markets: dict, show_volume: bool = True) -> str:
    """
    Build a code-block table of ALL markets for an event, sorted by price desc.
    """
    sorted_markets = sorted(
        markets.values(),
        key=lambda m: m.get("last_price") or 0,
        reverse=True,
    )

    lines: list[str] = []

    # Header
    if show_volume:
        lines.append(f"{'Model':<20} {'Last':>6} {'Bid':>6} {'Ask':>6} {'Vol24h':>7}")
        lines.append("-" * 49)
    else:
        lines.append(f"{'Model':<20} {'Last':>6} {'Bid':>6} {'Ask':>6}")
        lines.append("-" * 42)

    for m in sorted_markets:
        name = (m.get("name") or "Unknown")[:20]
        last = m.get("last_price")
        bid = m.get("yes_bid")
        ask = m.get("yes_ask")

        last_s = f"{last}¢" if last is not None else "—"
        bid_s = f"{bid}¢" if bid is not None else "—"
        ask_s = f"{ask}¢" if ask is not None else "—"

        if show_volume:
            vol = m.get("volume_24h")
            vol_s = str(vol) if vol is not None else "—"
            lines.append(f"{name:<20} {last_s:>6} {bid_s:>6} {ask_s:>6} {vol_s:>7}")
        else:
            lines.append(f"{name:<20} {last_s:>6} {bid_s:>6} {ask_s:>6}")

    return "\n".join(lines)


def build_embeds(
    changes: dict, new_snapshot: dict, force_send: bool
) -> list[list[dict]]:
    """
    Build Discord embed payloads from detected changes.

    Returns a list of "messages", where each message is a list of embeds.
    Discord allows max 10 embeds per message, so we batch accordingly.
    """
    all_embeds: list[dict] = []
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # --- New events (sorted by close time: weekly before monthly) ---
    new_events_sorted = sorted(
        changes.get("new_events", []),
        key=lambda et: event_sort_key(et, new_snapshot),
    )
    for et in new_events_sorted:
        ev = new_snapshot.get(et, {})
        series = ev.get("series_ticker", "")
        label = SERIES_CONFIG.get(series, {}).get("label", series)
        title = ev.get("title", et)
        url = event_web_url(series, et)
        markets = ev.get("markets", {})
        close_time = ""
        if markets:
            # Use close time from first market
            first_market = next(iter(markets.values()))
            close_time = format_close_time(first_market.get("close_time", ""))

        table = build_market_table(markets)

        description = f"**[{title}]({url})**\n"
        if close_time:
            description += f"Closes: {close_time}\n"
        description += f"Options: {len(markets)}\n"
        description += f"```\n{table}\n```"

        all_embeds.append(
            {
                "title": f"New Event: {label}",
                "description": description,
                "color": COLOR_NEW_EVENT,
                "footer": {"text": f"{et} | {now_str}"},
            }
        )

    # --- Removed events ---
    if changes.get("removed_events"):
        lines = []
        for et in changes["removed_events"]:
            lines.append(f"- `{et}`")
        all_embeds.append(
            {
                "title": "Events Closed / Removed",
                "description": "\n".join(lines),
                "color": COLOR_REMOVED,
                "footer": {"text": now_str},
            }
        )

    # --- New markets (options) within existing events ---
    if changes.get("new_markets"):
        lines = []
        for et, mt in changes["new_markets"]:
            ev = new_snapshot.get(et, {})
            m = ev.get("markets", {}).get(mt, {})
            name = m.get("name", mt)
            last = cents_str(m.get("last_price"))
            bid = cents_str(m.get("yes_bid"))
            ask = cents_str(m.get("yes_ask"))
            series = ev.get("series_ticker", "")
            url = event_web_url(series, et)
            lines.append(
                f"**{name}** added to [{et}]({url})\n"
                f"Last: {last} | Bid: {bid} | Ask: {ask}"
            )
        all_embeds.append(
            {
                "title": "New Options Added",
                "description": "\n\n".join(lines),
                "color": COLOR_NEW_OPTION,
                "footer": {"text": now_str},
            }
        )

    # --- Price changes ---
    if changes.get("price_changes"):
        sorted_changes = sorted(
            changes["price_changes"], key=lambda x: abs(x["delta"]), reverse=True
        )
        lines = []
        for pc in sorted_changes:
            direction = "+" if pc["delta"] > 0 else ""
            name = pc["name"]
            old_p = cents_str(pc["old_price"])
            new_p = cents_str(pc["new_price"])
            delta = pc["delta"]
            et = pc["event_ticker"]
            ev = new_snapshot.get(et, {})
            series = ev.get("series_ticker", "")
            url = event_web_url(series, et)
            vol = pc.get("volume_24h")
            vol_str = f" | 24h vol: {vol}" if vol else ""
            lines.append(
                f"**{name}** ({direction}{delta}¢): {old_p} → {new_p}{vol_str}\n"
                f"[{et}]({url})"
            )
        all_embeds.append(
            {
                "title": "Price Movements",
                "description": "\n\n".join(lines),
                "color": COLOR_PRICE_MOVE,
                "footer": {"text": now_str},
            }
        )

    # --- Settled markets ---
    if changes.get("settled_markets"):
        lines = []
        for et, mt, result in changes["settled_markets"]:
            lines.append(f"**{result}** — `{et}`")
        all_embeds.append(
            {
                "title": "Markets Settled",
                "description": "\n".join(lines),
                "color": COLOR_SETTLED,
                "footer": {"text": now_str},
            }
        )

    # --- Force send: full summary (sorted by close time: weekly before monthly) ---
    if not all_embeds and force_send:
        sorted_event_tickers = sorted(
            new_snapshot.keys(),
            key=lambda et: event_sort_key(et, new_snapshot),
        )
        for et in sorted_event_tickers:
            ev = new_snapshot[et]
            series = ev.get("series_ticker", "")
            label = SERIES_CONFIG.get(series, {}).get("label", series)
            title = ev.get("title", et)
            url = event_web_url(series, et)
            markets = ev.get("markets", {})
            close_time = ""
            if markets:
                first_market = next(iter(markets.values()))
                close_time = format_close_time(first_market.get("close_time", ""))

            table = build_market_table(markets)

            description = f"**[{title}]({url})**\n"
            if close_time:
                description += f"Closes: {close_time}\n"
            description += f"Options: {len(markets)}\n"
            description += f"```\n{table}\n```"

            all_embeds.append(
                {
                    "title": f"{label}: {et}",
                    "description": description,
                    "color": COLOR_SUMMARY,
                    "footer": {"text": now_str},
                }
            )

    # Batch into messages of up to 10 embeds each (Discord limit)
    messages: list[list[dict]] = []
    for i in range(0, len(all_embeds), 10):
        messages.append(all_embeds[i : i + 10])

    return messages


def send_discord_embeds(
    webhook_url: str,
    embed_messages: list[list[dict]],
    *,
    retries: int = 3,
    backoff: float = 2.0,
) -> bool:
    """Post embed messages to a Discord webhook. Returns True if all succeed."""
    if not validate_webhook_url(webhook_url):
        logger.error("Invalid Discord webhook URL")
        return False

    all_ok = True
    for msg_idx, embeds in enumerate(embed_messages):
        payload = {"embeds": embeds}
        success = _post_discord_payload(
            webhook_url, payload, retries=retries, backoff=backoff
        )
        if not success:
            all_ok = False
        # Small delay between multi-message sends to avoid rate limits
        if msg_idx < len(embed_messages) - 1:
            time.sleep(1.5)

    return all_ok


def _post_discord_payload(
    webhook_url: str,
    payload: dict,
    *,
    retries: int = 3,
    backoff: float = 2.0,
) -> bool:
    """Post a JSON payload to a Discord webhook with retries. Returns True on success."""
    last_exc: Exception | None = None

    for attempt in range(retries + 1):
        try:
            resp = requests.post(
                webhook_url,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 204:
                logger.info("Discord message sent successfully")
                return True
            if resp.status_code == 429:
                retry_after = resp.json().get("retry_after", 5)
                logger.warning("Discord rate limited, waiting %.1fs", retry_after)
                time.sleep(retry_after)
                continue
            if resp.status_code >= 500:
                last_exc = requests.exceptions.HTTPError(
                    f"Discord server error: {resp.status_code}"
                )
            else:
                logger.error(
                    "Discord webhook error %s: %s", resp.status_code, resp.text
                )
                return False
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc

        if attempt < retries:
            wait = backoff * (2**attempt) + random.uniform(0, 1)
            logger.warning(
                "Discord send failed (attempt %d/%d), retrying in %.1fs: %s",
                attempt + 1,
                retries + 1,
                wait,
                last_exc,
            )
            time.sleep(wait)

    logger.error("All attempts to send Discord message failed: %s", last_exc)
    return False


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def load_state(state_file: str) -> dict:
    path = Path(state_file)
    if not path.exists():
        logger.info("No existing state file at %s — starting fresh", state_file)
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load state file: %s — starting fresh", exc)
        return {}


def save_state(state_file: str, state: dict) -> None:
    path = Path(state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    logger.info("State saved to %s", state_file)


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------


def run_single_check(args: argparse.Namespace) -> bool:
    """
    Run a single monitoring check across all configured series.

    Returns True if a notification was sent (or would have been sent in dry-run).
    """
    old_state = load_state(args.state_file)
    old_snapshot = filter_yearly_events(old_state.get("snapshot", {}))

    # Fetch current data from all series
    new_snapshot: dict[str, Any] = {}
    for series_ticker in SERIES_CONFIG:
        logger.info("Fetching events for %s ...", series_ticker)
        try:
            events = fetch_series_events(
                series_ticker,
                timeout=args.timeout_seconds,
                retries=args.retries,
                backoff=args.retry_backoff_seconds,
            )
            logger.info(
                "  Found %d open event(s) for %s", len(events), series_ticker
            )
            series_snapshot = build_snapshot(events)
            new_snapshot.update(series_snapshot)
        except Exception as exc:
            logger.error("Failed to fetch %s: %s", series_ticker, exc)
            # Keep old data for this series so we don't false-alarm
            for et, ev in old_snapshot.items():
                if ev.get("series_ticker") == series_ticker:
                    new_snapshot[et] = ev

    if not new_snapshot:
        logger.warning("No event data retrieved — skipping this check")
        return False

    # Filter out yearly/year-end events
    before_count = len(new_snapshot)
    new_snapshot = filter_yearly_events(new_snapshot)
    filtered_count = before_count - len(new_snapshot)
    if filtered_count:
        logger.info("Filtered out %d yearly event(s)", filtered_count)

    # Compute diff
    changes = diff_snapshots(old_snapshot, new_snapshot, args.price_threshold)
    changed = has_changes(changes)

    if changed:
        logger.info("Changes detected:")
        for key, val in changes.items():
            if val:
                logger.info("  %s: %d item(s)", key, len(val))
    else:
        logger.info("No significant changes detected")

    # Build and send embeds
    should_send = changed or args.force_send
    if should_send:
        embed_messages = build_embeds(changes, new_snapshot, args.force_send)
        if embed_messages:
            if args.dry_run:
                logger.info(
                    "DRY RUN — would send %d message(s) with %d embed(s)",
                    len(embed_messages),
                    sum(len(m) for m in embed_messages),
                )
                for i, embeds in enumerate(embed_messages):
                    for e in embeds:
                        logger.info(
                            "  [msg %d] %s: %s",
                            i + 1,
                            e.get("title", ""),
                            e.get("description", "")[:200],
                        )
            else:
                if not args.webhook_url:
                    logger.error(
                        "No webhook URL provided. Set DISCORD_WEBHOOK_URL or use --webhook-url"
                    )
                else:
                    send_discord_embeds(
                        args.webhook_url,
                        embed_messages,
                        retries=args.retries,
                        backoff=args.retry_backoff_seconds,
                    )

    # Persist new state
    new_state = {
        "snapshot": new_snapshot,
        "last_check": datetime.now(timezone.utc).isoformat(),
        "last_changed": (
            datetime.now(timezone.utc).isoformat()
            if changed
            else old_state.get("last_changed", "")
        ),
    }
    save_state(args.state_file, new_state)

    return should_send


def main() -> None:
    args = parse_args()

    if args.force_send:
        args.max_checks = 1

    if args.loop:
        for i in range(args.max_checks):
            logger.info("=== Check %d/%d ===", i + 1, args.max_checks)
            try:
                run_single_check(args)
            except Exception:
                logger.exception("Check %d failed with unexpected error", i + 1)

            if i < args.max_checks - 1:
                interval = random.randint(
                    args.min_interval_seconds, args.max_interval_seconds
                )
                logger.info("Sleeping %ds before next check ...", interval)
                time.sleep(interval)
    else:
        run_single_check(args)

    logger.info("Done.")


if __name__ == "__main__":
    main()
