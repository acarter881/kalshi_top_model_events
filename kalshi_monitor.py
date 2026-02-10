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
import hashlib
import json
import logging
import os
import random
import sys
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
DISCORD_MESSAGE_LIMIT = 1800
REQUEST_TIMEOUT = 30


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
            markets[ticker] = {
                "title": m.get("title", ""),
                "subtitle": m.get("subtitle", ""),
                "status": m.get("status", ""),
                "yes_bid": m.get("yes_bid"),
                "yes_ask": m.get("yes_ask"),
                "last_price": m.get("last_price"),
                "volume": m.get("volume"),
                "volume_24h": m.get("volume_24h"),
                "open_interest": m.get("open_interest"),
                "close_time": m.get("close_time", ""),
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
                changes["settled_markets"].append((et, mt, nm.get("subtitle", "")))
                continue

            if old_price is not None and new_price is not None:
                delta = new_price - old_price
                if abs(delta) >= price_threshold:
                    changes["price_changes"].append(
                        {
                            "event_ticker": et,
                            "market_ticker": mt,
                            "subtitle": nm.get("subtitle", ""),
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


def price_display(cents: int | None) -> str:
    """Format a cent price as a dollar string with implied probability."""
    if cents is None:
        return "N/A"
    return f"${cents / 100:.2f} ({cents}%)"


def event_web_url(series_ticker: str, event_ticker: str) -> str:
    """Build a Kalshi web URL for an event."""
    cfg = SERIES_CONFIG.get(series_ticker, {})
    slug = cfg.get("slug", series_ticker.lower())
    return f"{KALSHI_WEB_BASE}/{series_ticker.lower()}/{slug}/{event_ticker.lower()}"


def build_message(
    changes: dict, new_snapshot: dict, force_send: bool
) -> str:
    """Build a Discord notification message from detected changes."""
    sections: list[str] = []
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # --- New events ---
    if changes["new_events"]:
        lines = [f"**New Events Created** ({now_str})"]
        for et in changes["new_events"]:
            ev = new_snapshot.get(et, {})
            series = ev.get("series_ticker", "")
            label = SERIES_CONFIG.get(series, {}).get("label", series)
            title = ev.get("title", et)
            url = event_web_url(series, et)
            market_count = len(ev.get("markets", {}))
            lines.append(f"• **{label}**: [{title}]({url}) — {market_count} options")

            # Show top options by price for new events
            markets = ev.get("markets", {})
            top_markets = sorted(
                markets.values(),
                key=lambda m: m.get("last_price") or 0,
                reverse=True,
            )[:5]
            for m in top_markets:
                sub = m.get("subtitle", "Unknown")
                lp = price_display(m.get("last_price"))
                lines.append(f"  └ {sub}: {lp}")

        sections.append("\n".join(lines))

    # --- Removed events ---
    if changes["removed_events"]:
        lines = [f"**Events Closed/Removed** ({now_str})"]
        for et in changes["removed_events"]:
            lines.append(f"• `{et}`")
        sections.append("\n".join(lines))

    # --- New markets (options) within existing events ---
    if changes["new_markets"]:
        lines = [f"**New Options Added** ({now_str})"]
        for et, mt in changes["new_markets"]:
            ev = new_snapshot.get(et, {})
            m = ev.get("markets", {}).get(mt, {})
            sub = m.get("subtitle", mt)
            lp = price_display(m.get("last_price"))
            series = ev.get("series_ticker", "")
            url = event_web_url(series, et)
            lines.append(f"• [{et}]({url}): **{sub}** at {lp}")
        sections.append("\n".join(lines))

    # --- Price changes ---
    if changes["price_changes"]:
        # Sort by absolute delta descending
        sorted_changes = sorted(
            changes["price_changes"], key=lambda x: abs(x["delta"]), reverse=True
        )
        lines = [f"**Price Movements** ({now_str})"]
        for pc in sorted_changes:
            direction = "↑" if pc["delta"] > 0 else "↓"
            sub = pc["subtitle"]
            old_p = price_display(pc["old_price"])
            new_p = price_display(pc["new_price"])
            delta_abs = abs(pc["delta"])
            vol = pc.get("volume_24h")
            vol_str = f" (24h vol: {vol})" if vol else ""
            lines.append(
                f"• **{sub}** {direction} {delta_abs}¢: {old_p} → {new_p}{vol_str}"
            )
            # Add event context
            et = pc["event_ticker"]
            ev = new_snapshot.get(et, {})
            series = ev.get("series_ticker", "")
            url = event_web_url(series, et)
            lines.append(f"  └ [{et}]({url})")
        sections.append("\n".join(lines))

    # --- Settled markets ---
    if changes["settled_markets"]:
        lines = [f"**Markets Settled** ({now_str})"]
        for et, mt, result in changes["settled_markets"]:
            lines.append(f"• `{et}` — **{result}** settled")
        sections.append("\n".join(lines))

    if not sections and force_send:
        # Force send: show current state summary
        lines = [f"**Kalshi AI Markets Summary** ({now_str})"]
        for et, ev in sorted(new_snapshot.items()):
            series = ev.get("series_ticker", "")
            label = SERIES_CONFIG.get(series, {}).get("label", series)
            url = event_web_url(series, et)
            lines.append(f"\n**[{label}: {et}]({url})**")

            markets = ev.get("markets", {})
            top_markets = sorted(
                markets.values(),
                key=lambda m: m.get("last_price") or 0,
                reverse=True,
            )[:5]
            for m in top_markets:
                sub = m.get("subtitle", "Unknown")
                lp = price_display(m.get("last_price"))
                bid = price_display(m.get("yes_bid"))
                ask = price_display(m.get("yes_ask"))
                lines.append(f"  {sub}: Last {lp} | Bid {bid} | Ask {ask}")

        sections.append("\n".join(lines))

    message = "\n\n".join(sections)

    # Truncate if needed
    if len(message) > DISCORD_MESSAGE_LIMIT:
        truncated_note = "\n\n*(Message truncated — see Kalshi for full details)*"
        message = message[: DISCORD_MESSAGE_LIMIT - len(truncated_note)] + truncated_note

    return message


def send_discord_message(
    webhook_url: str,
    message: str,
    *,
    retries: int = 3,
    backoff: float = 2.0,
) -> bool:
    """Post a message to a Discord webhook. Returns True on success."""
    if not validate_webhook_url(webhook_url):
        logger.error("Invalid Discord webhook URL")
        return False

    payload = {"content": message}
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
                # Rate limited — respect retry_after
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
    old_snapshot = old_state.get("snapshot", {})

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

    # Build and send message
    should_send = changed or args.force_send
    if should_send:
        message = build_message(changes, new_snapshot, args.force_send)
        if message.strip():
            if args.dry_run:
                logger.info("DRY RUN — would send:\n%s", message)
            else:
                if not args.webhook_url:
                    logger.error(
                        "No webhook URL provided. Set DISCORD_WEBHOOK_URL or use --webhook-url"
                    )
                    # Still save state so we don't re-alert
                else:
                    send_discord_message(
                        args.webhook_url,
                        message,
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
