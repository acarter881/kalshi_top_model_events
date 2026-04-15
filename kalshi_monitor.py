"""
Kalshi AI Model Event Monitor

Monitors Kalshi prediction market events for AI model rankings and sends
Discord notifications when significant changes are detected, including:
- New events created (e.g., new weekly/monthly market)
- Significant price movements on existing contracts
- New market options added to existing events
- Markets settled/resolved
- Settlement source changes (e.g., data source URL swap)
- Contract terms PDF updates (rule changes)
"""

import argparse
import hashlib
import json
import logging
import os
import random
import time
from datetime import datetime, timezone, timedelta
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
        "description": "#1 ranked model on LM Arena",
        "contract_terms_url": "https://kalshi-public-docs.s3.amazonaws.com/contract_terms/TOPMODEL.pdf",
        "sort_order": 0,
    },
    "KXLLM1": {
        "slug": "yearend-top-llm",
        "label": "Best AI",
        "description": "Best overall AI model of the period",
        "contract_terms_url": "https://kalshi-public-docs.s3.amazonaws.com/contract_terms/LLM1.pdf",
        "sort_order": 1,
    },
}

DEFAULT_STATE_FILE = ".github/state/kalshi_state.json"
DEFAULT_PRICE_THRESHOLD = 5  # cents
REQUEST_TIMEOUT = 30

# US Central time (CST/CDT) — UTC-6 standard, UTC-5 daylight
CST = timezone(timedelta(hours=-6))

# Keywords in event titles that indicate a yearly event (case-insensitive)
YEARLY_TITLE_KEYWORDS = ("year", "annual", "end of 202")

# Change types that trigger Discord notifications. Change types NOT in this
# set (new_events, removed_events, price_changes) are still detected and
# logged, but are silenced — they are noise (scheduled events, price churn)
# that the user doesn't want to be pinged about. Only series-level structural
# changes (new model added, settlement rules changed, etc.) produce alerts.
ALERT_CHANGE_TYPES = frozenset(
    {
        "new_markets",
        "removed_markets",
        "settled_markets",
        "settlement_source_changes",
        "pdf_changes",
    }
)


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


def fetch_event_metadata(
    event_ticker: str,
    *,
    timeout: int = REQUEST_TIMEOUT,
    retries: int = 3,
    backoff: float = 2.0,
) -> dict:
    """Fetch metadata for a specific event, including settlement sources."""
    url = f"{KALSHI_API_BASE}/events/{event_ticker}/metadata"
    return fetch_with_retries(url, timeout=timeout, retries=retries, backoff=backoff)


def fetch_pdf_hash(
    url: str,
    *,
    timeout: int = REQUEST_TIMEOUT,
    retries: int = 3,
    backoff: float = 2.0,
) -> str | None:
    """Download a PDF and return its SHA-256 hex digest, or None on failure."""
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return hashlib.sha256(resp.content).hexdigest()
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt < retries:
                wait = backoff * (2**attempt) + random.uniform(0, 1)
                logger.warning(
                    "PDF fetch from %s failed (attempt %d/%d), retrying in %.1fs: %s",
                    url,
                    attempt + 1,
                    retries + 1,
                    wait,
                    last_exc,
                )
                time.sleep(wait)

    logger.error("Failed to fetch PDF from %s: %s", url, last_exc)
    return None


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


def _dollars_to_cents(val: Any) -> int | None:
    """Convert a dollar-string like '0.5600' to integer cents (56), or None."""
    if val is None:
        return None
    try:
        return round(float(val) * 100)
    except (ValueError, TypeError):
        return None


def _fp_to_int(val: Any) -> int | None:
    """Convert a fixed-point string like '1234.00' to integer, or None."""
    if val is None:
        return None
    try:
        return round(float(val))
    except (ValueError, TypeError):
        return None


def build_snapshot(events: list[dict]) -> dict:
    """
    Build a normalized snapshot from raw Kalshi events.

    Returns a dict keyed by event_ticker, each containing:
      - event metadata (title, dates)
      - markets dict keyed by market ticker with prices / volume

    Note: Kalshi migrated from integer-cent fields (yes_bid, yes_ask, etc.)
    to dollar-string fields (yes_bid_dollars, yes_ask_dollars, etc.) as of
    March 12, 2026. We read the new _dollars fields first, falling back to
    the legacy integer fields for backward compatibility with cached state.
    """
    snapshot: dict[str, Any] = {}
    for event in events:
        event_ticker = event.get("event_ticker", "")
        markets_raw = event.get("markets") or []

        markets: dict[str, Any] = {}
        for m in markets_raw:
            ticker = m.get("ticker", "")
            name = derive_market_name(m)

            # Prefer new _dollars fields, fall back to legacy integer fields
            yes_bid = _dollars_to_cents(m.get("yes_bid_dollars")) if m.get("yes_bid_dollars") is not None else m.get("yes_bid")
            yes_ask = _dollars_to_cents(m.get("yes_ask_dollars")) if m.get("yes_ask_dollars") is not None else m.get("yes_ask")
            last_price = _dollars_to_cents(m.get("last_price_dollars")) if m.get("last_price_dollars") is not None else m.get("last_price")
            volume = _fp_to_int(m.get("volume_fp")) if m.get("volume_fp") is not None else m.get("volume")
            volume_24h = _fp_to_int(m.get("volume_24h_fp")) if m.get("volume_24h_fp") is not None else m.get("volume_24h")
            open_interest = _fp_to_int(m.get("open_interest_fp")) if m.get("open_interest_fp") is not None else m.get("open_interest")

            markets[ticker] = {
                "name": name,
                "title": m.get("title", ""),
                "subtitle": m.get("subtitle", ""),
                "yes_sub_title": m.get("yes_sub_title", ""),
                "status": m.get("status", ""),
                "yes_bid": yes_bid,
                "yes_ask": yes_ask,
                "last_price": last_price,
                "volume": volume,
                "volume_24h": volume_24h,
                "open_interest": open_interest,
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


def derive_event_period(event: dict) -> str:
    """
    Derive 'Weekly' or 'Monthly' from an event's title/subtitle or close time.

    Heuristics (in order):
      1. If title or subtitle contains 'week' → Weekly
      2. If title or subtitle contains 'month' → Monthly
      3. If close time falls on the last 3 days of the month → Monthly
      4. Otherwise → Weekly
    """
    title = (event.get("title") or "").lower()
    sub_title = (event.get("sub_title") or "").lower()

    if "week" in title or "week" in sub_title:
        return "Weekly"
    if "month" in title or "month" in sub_title:
        return "Monthly"

    # Fall back to close time: last 3 days of month → Monthly
    markets = event.get("markets", {})
    if markets:
        first_market = next(iter(markets.values()))
        close_str = first_market.get("close_time", "")
        if close_str:
            try:
                dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                # Check if close date is in the last 3 days of its month
                next_month = (dt.replace(day=28) + timedelta(days=4)).replace(day=1)
                last_day = (next_month - timedelta(days=1)).day
                if dt.day >= last_day - 2:
                    return "Monthly"
            except (ValueError, TypeError):
                pass

    return "Weekly"


def format_event_label(event_ticker: str, event: dict) -> str:
    """
    Build a human-readable label like 'Top AI Model — Weekly (Feb 14)'.
    """
    series = event.get("series_ticker", "")
    cfg = SERIES_CONFIG.get(series, {})
    label = cfg.get("label", series)
    period = derive_event_period(event)

    # Extract a short date from the close time
    date_str = ""
    markets = event.get("markets", {})
    if markets:
        first_market = next(iter(markets.values()))
        close = first_market.get("close_time", "")
        if close:
            try:
                dt = datetime.fromisoformat(close.replace("Z", "+00:00"))
                date_str = dt.strftime("%b %-d")
            except (ValueError, TypeError):
                pass

    parts = [label, period]
    result = " — ".join(parts)
    if date_str:
        result += f" ({date_str})"
    return result


def event_sort_key(event_ticker: str, snapshot: dict) -> tuple:
    """
    Return a sort key that groups events by series, then by close time.

    This produces: all KXTOPMODEL events (weekly before monthly),
    then all KXLLM1 events (weekly before monthly).
    """
    ev = snapshot.get(event_ticker, {})
    series = ev.get("series_ticker", "")
    series_order = SERIES_CONFIG.get(series, {}).get("sort_order", 99)

    close_time = ""
    markets = ev.get("markets", {})
    if markets:
        first_market = next(iter(markets.values()))
        close_time = first_market.get("close_time", "")

    time_key = close_time or ev.get("strike_date", "") or event_ticker
    return (series_order, time_key)


# ---------------------------------------------------------------------------
# Diffing
# ---------------------------------------------------------------------------


def diff_snapshots(
    old: dict, new: dict, price_threshold: int
) -> dict[str, Any]:
    """
    Compare two snapshots and return structured changes.

    Returns dict with keys:
      new_events                – list of event tickers that are brand new
      removed_events            – list of event tickers that disappeared
      new_markets               – list of (event_ticker, market_ticker) for new options
      removed_markets           – list of (event_ticker, market_ticker)
      price_changes             – list of dicts with price movement details
      settled_markets           – list of (event_ticker, market_ticker, result)
      settlement_source_changes – list of dicts with event_ticker, old/new sources
    """
    changes: dict[str, list] = {
        "new_events": [],
        "removed_events": [],
        "new_markets": [],
        "removed_markets": [],
        "price_changes": [],
        "settled_markets": [],
        "settlement_source_changes": [],
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

        # Price changes on shared markets (based on yes_ask — the current
        # market price Kalshi displays, not the potentially stale last_price)
        for mt in sorted(old_mt & new_mt):
            om = old_markets[mt]
            nm = new_markets[mt]

            # Detect settlement
            if om.get("status") != "settled" and nm.get("status") == "settled":
                changes["settled_markets"].append((et, mt, nm.get("name", mt)))
                continue

            old_price = om.get("yes_ask")
            new_price = nm.get("yes_ask")

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

        # Settlement source changes
        old_sources = old[et].get("settlement_sources") or []
        new_sources = new[et].get("settlement_sources") or []
        # Normalize for comparison: sort by name to ignore ordering changes
        old_sorted = sorted(old_sources, key=lambda s: s.get("name", ""))
        new_sorted = sorted(new_sources, key=lambda s: s.get("name", ""))
        if old_sorted != new_sorted and old_sources:
            # Only alert if we had old sources (skip first-time population)
            changes["settlement_source_changes"].append(
                {
                    "event_ticker": et,
                    "title": new[et].get("title", et),
                    "old_sources": old_sources,
                    "new_sources": new_sources,
                }
            )

    return changes


def diff_pdf_hashes(
    old_hashes: dict[str, str], new_hashes: dict[str, str]
) -> list[dict]:
    """Compare old and new PDF hashes. Returns list of changed series."""
    pdf_changes: list[dict] = []
    for series_ticker, new_hash in new_hashes.items():
        old_hash = old_hashes.get(series_ticker)
        if old_hash and old_hash != new_hash:
            cfg = SERIES_CONFIG.get(series_ticker, {})
            pdf_changes.append(
                {
                    "series_ticker": series_ticker,
                    "label": cfg.get("label", series_ticker),
                    "contract_terms_url": cfg.get("contract_terms_url", ""),
                }
            )
    return pdf_changes


def has_changes(changes: dict) -> bool:
    """
    Return True if any alert-worthy change type has items.

    Only change types in ALERT_CHANGE_TYPES count — silenced types like
    price_changes and new_events are detected by diff_snapshots() but do
    not trigger notifications.
    """
    return any(bool(changes.get(k)) for k in ALERT_CHANGE_TYPES)


def has_any_changes(changes: dict) -> bool:
    """Return True if any change type (including silenced ones) has items."""
    return any(bool(v) for v in changes.values())


def filter_alert_changes(changes: dict) -> dict:
    """
    Return a copy of `changes` with non-alert change types cleared.

    Used to hide silenced types from build_embeds() so they never appear
    in Discord messages, while preserving the full diff for logging.
    """
    return {k: (v if k in ALERT_CHANGE_TYPES else []) for k, v in changes.items()}


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
    """Format a cent price as a readable string: 57¢."""
    if cents is None:
        return "—"
    return f"{cents}¢"


def event_web_url(series_ticker: str, event_ticker: str) -> str:
    """Build a Kalshi web URL for an event."""
    cfg = SERIES_CONFIG.get(series_ticker, {})
    slug = cfg.get("slug", series_ticker.lower())
    return f"{KALSHI_WEB_BASE}/{series_ticker.lower()}/{slug}/{event_ticker.lower()}"


def format_close_time(close_time: str) -> str:
    """Format an ISO close time to a short human string in CST."""
    if not close_time:
        return ""
    try:
        dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
        dt_cst = dt.astimezone(CST)
        return dt_cst.strftime("%b %d, %Y %-I:%M %p CST")
    except (ValueError, TypeError):
        return close_time


# Discord embed color palette
COLOR_NEW_EVENT = 0x57F287  # green
COLOR_PRICE_MOVE = 0xFEE75C  # yellow
COLOR_NEW_OPTION = 0x5865F2  # blurple
COLOR_SETTLED = 0xEB459E  # pink/red
COLOR_REMOVED = 0xED4245  # red
COLOR_SUMMARY = 0x5865F2  # blurple
COLOR_RULES_CHANGE = 0xFF9900  # orange


def build_market_table(markets: dict) -> str:
    """
    Build a code-block table of ALL markets for an event, sorted by Yes price desc.
    """
    sorted_markets = sorted(
        markets.values(),
        key=lambda m: m.get("yes_ask") or m.get("last_price") or 0,
        reverse=True,
    )

    lines: list[str] = []
    lines.append(f"{'Model':<20} {'Yes':>6} {'Bid':>6} {'Last':>6}")
    lines.append("-" * 42)

    for m in sorted_markets:
        name = (m.get("name") or "Unknown")[:20]
        ask = m.get("yes_ask")
        bid = m.get("yes_bid")
        last = m.get("last_price")

        ask_s = f"{ask}¢" if ask is not None else "—"
        bid_s = f"{bid}¢" if bid is not None else "—"
        last_s = f"{last}¢" if last is not None else "—"

        lines.append(f"{name:<20} {ask_s:>6} {bid_s:>6} {last_s:>6}")

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
    now_str = datetime.now(CST).strftime("%b %d, %Y %-I:%M %p CST")

    # --- New events (sorted by close time: weekly before monthly) ---
    new_events_sorted = sorted(
        changes.get("new_events", []),
        key=lambda et: event_sort_key(et, new_snapshot),
    )
    for et in new_events_sorted:
        ev = new_snapshot.get(et, {})
        series = ev.get("series_ticker", "")
        friendly = format_event_label(et, ev)
        url = event_web_url(series, et)
        markets = ev.get("markets", {})
        close_time = ""
        if markets:
            first_market = next(iter(markets.values()))
            close_time = format_close_time(first_market.get("close_time", ""))

        table = build_market_table(markets)

        description = f"**[{friendly}]({url})**\n"
        if close_time:
            description += f"Closes: {close_time}\n"
        description += f"Options: {len(markets)}\n"
        description += f"```\n{table}\n```"

        all_embeds.append(
            {
                "title": f"New Event: {friendly}",
                "description": description,
                "color": COLOR_NEW_EVENT,
                "footer": {"text": now_str},
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
            yes = cents_str(m.get("yes_ask"))
            bid = cents_str(m.get("yes_bid"))
            series = ev.get("series_ticker", "")
            friendly = format_event_label(et, ev)
            url = event_web_url(series, et)
            lines.append(
                f"**{name}** added to [{friendly}]({url})\n"
                f"Yes: {yes} | Bid: {bid}"
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
            friendly = format_event_label(et, ev)
            url = event_web_url(series, et)
            vol = pc.get("volume_24h") or 0
            vol_str = f" | 24h vol: {vol:,}" if vol >= 1000 else ""
            lines.append(
                f"**{name}** ({direction}{delta}¢): {old_p} → {new_p}{vol_str}\n"
                f"[{friendly}]({url})"
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

    # --- Settlement source changes ---
    if changes.get("settlement_source_changes"):
        lines = []
        for sc in changes["settlement_source_changes"]:
            et = sc["event_ticker"]
            title = sc["title"]
            old_src = sc["old_sources"]
            new_src = sc["new_sources"]

            old_names = {s.get("name", "") for s in old_src}
            new_names = {s.get("name", "") for s in new_src}
            new_map = {s.get("name", ""): s.get("url", "") for s in new_src}

            detail_parts = []
            for name in sorted(new_names - old_names):
                detail_parts.append(f"Added: [{name}]({new_map.get(name, '')})")
            for name in sorted(old_names - new_names):
                detail_parts.append(f"Removed: {name}")
            # URL changes for sources that exist in both
            for s_old in old_src:
                for s_new in new_src:
                    if (
                        s_old.get("name") == s_new.get("name")
                        and s_old.get("url") != s_new.get("url")
                    ):
                        detail_parts.append(
                            f"**{s_new['name']}** URL changed → [{s_new['url']}]({s_new['url']})"
                        )

            detail = "\n".join(detail_parts) if detail_parts else "Sources updated"
            lines.append(f"**{title}** (`{et}`)\n{detail}")

        all_embeds.append(
            {
                "title": "Settlement Source Changed",
                "description": "\n\n".join(lines),
                "color": COLOR_RULES_CHANGE,
                "footer": {"text": now_str},
            }
        )

    # --- Contract terms PDF changes ---
    if changes.get("pdf_changes"):
        lines = []
        for pc in changes["pdf_changes"]:
            label = pc["label"]
            url = pc.get("contract_terms_url", "")
            if url:
                lines.append(f"**{label}** contract terms updated — [View PDF]({url})")
            else:
                lines.append(f"**{label}** contract terms updated")
        all_embeds.append(
            {
                "title": "Contract Rules Updated",
                "description": "\n\n".join(lines),
                "color": COLOR_RULES_CHANGE,
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
            friendly = format_event_label(et, ev)
            url = event_web_url(series, et)
            markets = ev.get("markets", {})
            close_time = ""
            if markets:
                first_market = next(iter(markets.values()))
                close_time = format_close_time(first_market.get("close_time", ""))

            table = build_market_table(markets)

            description = f"**[{friendly}]({url})**\n"
            if close_time:
                description += f"Closes: {close_time}\n"
            description += f"Options: {len(markets)}\n"
            description += f"```\n{table}\n```"

            all_embeds.append(
                {
                    "title": friendly,
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

    # Fetch settlement sources for each event via metadata endpoint
    for et in new_snapshot:
        try:
            meta = fetch_event_metadata(
                et,
                timeout=args.timeout_seconds,
                retries=args.retries,
                backoff=args.retry_backoff_seconds,
            )
            sources = meta.get("settlement_sources") or []
            new_snapshot[et]["settlement_sources"] = sources
            if sources:
                logger.info(
                    "  %s settlement sources: %s",
                    et,
                    ", ".join(s.get("name", "?") for s in sources),
                )
        except Exception as exc:
            logger.warning("Failed to fetch metadata for %s: %s", et, exc)
            # Carry forward old sources if available to avoid false diffs
            old_sources = old_snapshot.get(et, {}).get("settlement_sources")
            if old_sources is not None:
                new_snapshot[et]["settlement_sources"] = old_sources

    # Check contract terms PDFs for changes
    old_pdf_hashes = old_state.get("pdf_hashes", {})
    new_pdf_hashes: dict[str, str] = {}
    for series_ticker, cfg in SERIES_CONFIG.items():
        pdf_url = cfg.get("contract_terms_url", "")
        if not pdf_url:
            continue
        logger.info("Checking contract terms PDF for %s ...", series_ticker)
        pdf_hash = fetch_pdf_hash(
            pdf_url,
            timeout=args.timeout_seconds,
            retries=args.retries,
            backoff=args.retry_backoff_seconds,
        )
        if pdf_hash:
            new_pdf_hashes[series_ticker] = pdf_hash
            logger.info("  %s PDF hash: %s", series_ticker, pdf_hash[:16])
        else:
            # Keep old hash on failure to avoid false diffs
            if series_ticker in old_pdf_hashes:
                new_pdf_hashes[series_ticker] = old_pdf_hashes[series_ticker]

    pdf_changes = diff_pdf_hashes(old_pdf_hashes, new_pdf_hashes)
    if pdf_changes:
        logger.info("Contract terms PDF changes detected for: %s",
                     ", ".join(pc["series_ticker"] for pc in pdf_changes))

    # Compute diff
    changes = diff_snapshots(old_snapshot, new_snapshot, args.price_threshold)
    changes["pdf_changes"] = pdf_changes
    changed = has_changes(changes)  # alert-worthy changes only

    if has_any_changes(changes):
        logger.info("Changes detected:")
        for key, val in changes.items():
            if val:
                marker = "[alert]" if key in ALERT_CHANGE_TYPES else "[silenced]"
                logger.info("  %s %s: %d item(s)", marker, key, len(val))
    else:
        logger.info("No changes detected")

    # Build and send embeds — silenced change types (price_changes, new_events,
    # removed_events) are filtered out so they never reach Discord, even if
    # other alert-worthy changes are also firing in the same run.
    send_failed = False
    should_send = changed or args.force_send
    if should_send:
        alert_only_changes = filter_alert_changes(changes)
        embed_messages = build_embeds(alert_only_changes, new_snapshot, args.force_send)
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
                    send_failed = True
                else:
                    ok = send_discord_embeds(
                        args.webhook_url,
                        embed_messages,
                        retries=args.retries,
                        backoff=args.retry_backoff_seconds,
                    )
                    if not ok:
                        send_failed = True
                        logger.error(
                            "Discord send failed — skipping state save so changes "
                            "will be retried on next run"
                        )

    # Only persist state if notifications were delivered (or there was nothing to send).
    # If Discord send failed, keep old state so changes are retried next run.
    if send_failed:
        logger.warning("State NOT saved due to Discord send failure")
    else:
        new_state = {
            "snapshot": new_snapshot,
            "pdf_hashes": new_pdf_hashes,
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
