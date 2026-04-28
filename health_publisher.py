#!/usr/bin/env python3
"""
health_publisher_v2.py
======================

Drop-in replacement for health_publisher.py in project-command-center.

What changes vs. v1
-------------------
1. Adds check_stuart_house() that reads data.json from the stuart-house-dashboard
   repo (or the local file if you're running alongside the scraper) and reports
   staleness.
2. Adds a self-heartbeat file heartbeat.json written every run, even if some
   project checks fail. This lets the GitHub Actions external watchdog detect
   the publisher dying.
3. Wraps each check in try/except so one broken check cannot silently kill
   the whole publisher. Failures become reported offline projects, not crashes.
4. Writes a "failures" field listing any check that raised, so the dashboard
   can show exactly what broke.
5. Refuses to publish if fewer than MIN_OK_CHECKS succeed — prevents git
   committing an empty health file when everything is down (which would
   look "green" to naive readers).

INSTALL
-------
1. Back up the current file:
     cp ~/Desktop/project-command-center/health_publisher.py \\
        ~/Desktop/project-command-center/health_publisher.py.bak
2. Copy this file in:
     cp health_publisher_v2.py ~/Desktop/project-command-center/health_publisher.py
3. Adjust STUART_HOUSE_DATA_PATH below if your local Stuart House sweep
   writes data.json somewhere other than the repo.
4. The existing launchd plist keeps working; no changes needed there.

The script is idempotent. Running it by hand is safe.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# ---------- CONFIG ----------

HOME = Path(os.path.expanduser("~"))
REPO = HOME / "Desktop" / "project-command-center"
HEALTH_FILE = REPO / "system_health.json"
HEARTBEAT_FILE = REPO / "heartbeat.json"

# Local file paths that other systems write to. If these are stale or missing,
# the corresponding project is reported offline.
SQUEEZE_HEALTH_PATH   = HOME / "Desktop" / "mikes-trading-bot" / "data" / "health.json"
BRIDGE_FEED_PATH      = HOME / "Desktop" / "intelligence-bridge" / "bridge-status" / "unified_feed.json"
SENTINEL_DB_PATH      = HOME / "Desktop" / "SentinelCompass" / "knowledge_base" / "compass.db"
STUART_HOUSE_DATA_PATH = HOME / "Desktop" / "stuart-house-dashboard" / "data.json"

# External URLs that we ping to verify published dashboards are serving
MOTEOPS_LANDING_URL = "https://mikedmote52.github.io/moteops-landing/sidegigbot.html"

# Thresholds
STALE_MINUTES_FAST  = 30    # trading/squeeze: should update every 15m
STALE_MINUTES_HOUSE = 180   # stuart house: 3h during business hours
STALE_MINUTES_SLOW  = 60    # sentinel, bridge: should update hourly

# Publisher sanity: only refuse to publish if EVERY single check failed
# (catastrophic failure of the publisher itself). If even one check returns
# real data, we publish the honest state — that includes honest "everything
# is offline" reports, which are exactly what the watchdog needs to see.
MIN_OK_CHECKS = 1


# ---------- UTILITIES ----------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def file_age_minutes(p: Path) -> float | None:
    try:
        mtime = p.stat().st_mtime
        age_sec = datetime.now().timestamp() - mtime
        return age_sec / 60.0
    except FileNotFoundError:
        return None

def read_json(p: Path) -> dict[str, Any] | None:
    try:
        with p.open("r") as f:
            return json.load(f)
    except Exception:
        return None

def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        # handle both "...Z" and offset forms
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None

def status_for_age(age_min: float | None, stale_threshold: float) -> tuple[str, int]:
    """Return (status, health_score_0_to_100) based on age."""
    if age_min is None:
        return "offline", 0
    if age_min < stale_threshold / 2:
        return "online", 100
    if age_min < stale_threshold:
        return "degraded", 70
    if age_min < stale_threshold * 3:
        return "stale", 40
    return "offline", 15


# ---------- INDIVIDUAL CHECKS ----------

def check_squeeze_prophet() -> dict[str, Any]:
    data = read_json(SQUEEZE_HEALTH_PATH)
    age = file_age_minutes(SQUEEZE_HEALTH_PATH)
    status, health = status_for_age(age, STALE_MINUTES_FAST)
    return {
        "status": status,
        "health": health,
        "last_updated_age_min": round(age, 1) if age is not None else None,
        "warnings": (data or {}).get("warnings", []) if data else ["health.json missing"],
        "notes": f"source={SQUEEZE_HEALTH_PATH.name}",
    }

def check_kalshi_intelligence() -> dict[str, Any]:
    # Original v1 checked Docker containers. Keep that, but wrap in try/except
    # and surface a real error instead of crashing.
    try:
        out = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10, check=False
        )
        names = [n for n in out.stdout.splitlines() if "kalshi" in n.lower()]
        if names:
            return {
                "status": "online",
                "health": 100,
                "containers": names,
                "warnings": [],
            }
        return {
            "status": "offline",
            "health": 20,
            "containers": [],
            "warnings": ["No Kalshi containers running"],
        }
    except FileNotFoundError:
        return {"status": "offline", "health": 0, "warnings": ["docker CLI not found"]}
    except subprocess.TimeoutExpired:
        return {"status": "degraded", "health": 30, "warnings": ["docker ps timed out"]}

def check_sentinelcompass() -> dict[str, Any]:
    age = file_age_minutes(SENTINEL_DB_PATH)
    status, health = status_for_age(age, STALE_MINUTES_SLOW)
    warnings = []
    if age is None:
        warnings.append("compass.db not found")
    elif age > STALE_MINUTES_SLOW:
        warnings.append(f"DB last updated {round(age/60, 1)}h ago")
    return {
        "status": status,
        "health": health,
        "db_age_min": round(age, 1) if age is not None else None,
        "warnings": warnings,
    }

def check_intelligence_bridge() -> dict[str, Any]:
    data = read_json(BRIDGE_FEED_PATH)
    age = file_age_minutes(BRIDGE_FEED_PATH)
    status, health = status_for_age(age, STALE_MINUTES_SLOW)
    return {
        "status": status,
        "health": health,
        "feed_age_min": round(age, 1) if age is not None else None,
        "warnings": [] if data else ["unified_feed.json unreadable"],
    }

def check_stuart_house() -> dict[str, Any]:
    """NEW in v2 — Stuart House was completely absent from v1."""
    data = read_json(STUART_HOUSE_DATA_PATH)
    if not data:
        return {
            "status": "offline",
            "health": 0,
            "warnings": ["data.json missing or unreadable"],
            "notes": f"expected at {STUART_HOUSE_DATA_PATH}",
        }

    # Prefer the explicit timestamp field over mtime
    last_updated = data.get("lastUpdated") or data.get("generated_at")
    ts = parse_iso(last_updated)
    if ts:
        age_min = (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds() / 60.0
    else:
        age_min = file_age_minutes(STUART_HOUSE_DATA_PATH)

    status, health = status_for_age(age_min, STALE_MINUTES_HOUSE)

    warnings = []
    # surface auth degradation signals if present in the sweep log
    listings = data.get("listings") or []
    logged_out = sum(1 for l in listings if "logged out" in str(l).lower())
    if logged_out:
        warnings.append(f"{logged_out} listings have logged-out platform sessions")
    if age_min and age_min > STALE_MINUTES_HOUSE:
        warnings.append(f"sweep data {round(age_min/60, 1)}h stale")

    # Count pending prospects so the command center shows workload
    prospects = data.get("prospects") or []
    pending = [p for p in prospects if isinstance(p, dict) and p.get("status", "").lower() in ("pending", "needs_reply", "awaiting_response")]

    return {
        "status": status,
        "health": health,
        "data_age_min": round(age_min, 1) if age_min is not None else None,
        "prospect_count": len(prospects),
        "pending_prospects": len(pending),
        "warnings": warnings,
    }

def check_mote_ops_landing() -> dict[str, Any]:
    try:
        import urllib.request
        req = urllib.request.Request(MOTEOPS_LANDING_URL, headers={"User-Agent": "health-publisher/2.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            code = resp.getcode()
        return {
            "status": "online" if code == 200 else "degraded",
            "health": 100 if code == 200 else 60,
            "http_code": code,
            "warnings": [] if code == 200 else [f"HTTP {code}"],
        }
    except Exception as e:
        return {"status": "offline", "health": 20, "warnings": [f"fetch failed: {e}"]}

def check_project_command_center() -> dict[str, Any]:
    # This publisher IS the command center. If this code runs at all, it's up.
    return {"status": "online", "health": 100, "warnings": []}


# ---------- ORCHESTRATION ----------

CHECKS: dict[str, Callable[[], dict[str, Any]]] = {
    "mikes-trading-bot":        check_squeeze_prophet,
    "claude-kalshi":            check_kalshi_intelligence,
    "sentinel-compass":         check_sentinelcompass,
    "intelligence-bridge":      check_intelligence_bridge,
    "stuart-house-manager":     check_stuart_house,        # NEW
    "side-gig-bot":             check_mote_ops_landing,
    "project-command-center":   check_project_command_center,
}

def run_all() -> tuple[dict[str, Any], list[str]]:
    projects: dict[str, Any] = {}
    failures: list[str] = []
    for name, fn in CHECKS.items():
        try:
            projects[name] = fn()
        except Exception as e:
            failures.append(f"{name}: {e.__class__.__name__}: {e}")
            projects[name] = {
                "status": "unknown",
                "health": 0,
                "warnings": [f"check crashed: {e}"],
                "traceback": traceback.format_exc(limit=3),
            }
    return projects, failures

def write_heartbeat(success: bool, message: str) -> None:
    """Always write this, even if publish is aborted."""
    hb = {
        "generated_at": now_utc_iso(),
        "success": success,
        "message": message,
        "hostname": os.uname().nodename,
        "python": sys.version.split()[0],
    }
    try:
        HEARTBEAT_FILE.write_text(json.dumps(hb, indent=2))
    except Exception as e:
        print(f"[heartbeat] failed to write: {e}", file=sys.stderr)

def git_commit_and_push() -> None:
    try:
        subprocess.run(["git", "-C", str(REPO), "add", "system_health.json", "heartbeat.json"],
                       check=True, capture_output=True)
        # Only commit if there's actually a change
        diff = subprocess.run(["git", "-C", str(REPO), "diff", "--cached", "--quiet"],
                              capture_output=True)
        if diff.returncode == 0:
            return  # nothing to commit
        subprocess.run(
            ["git", "-C", str(REPO), "commit", "-m", f"health: {now_utc_iso()}"],
            check=True, capture_output=True
        )
        subprocess.run(["git", "-C", str(REPO), "push"], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f"[git] commit/push failed: {e.stderr.decode() if e.stderr else e}", file=sys.stderr)
        raise

def main() -> int:
    projects, failures = run_all()

    # "ok" here means the check function executed without crashing, regardless
    # of whether the project it was checking is up. Checks that crashed
    # (status == "unknown") don't count.
    ok_count  = sum(1 for p in projects.values() if p.get("status") != "unknown")
    online_count = sum(1 for p in projects.values() if p.get("status") == "online")
    if ok_count < MIN_OK_CHECKS:
        msg = f"only {ok_count} checks executed successfully — publisher itself may be broken"
        print(f"[publisher] {msg}", file=sys.stderr)
        write_heartbeat(success=False, message=msg)
        # still commit heartbeat so the watchdog can see publisher ran
        try:
            subprocess.run(["git", "-C", str(REPO), "add", "heartbeat.json"], check=False)
            subprocess.run(["git", "-C", str(REPO), "commit", "-m", f"heartbeat: degraded {now_utc_iso()}"],
                           check=False, capture_output=True)
            subprocess.run(["git", "-C", str(REPO), "push"], check=False, capture_output=True)
        except Exception:
            pass
        return 2

    snapshot = {
        "generated_at": now_utc_iso(),
        "version": "2.0",
        "projects": projects,
        "failures": failures,
        "ok_count": online_count,       # kept name for backwards compat with dashboards
        "checks_executed": ok_count,
        "total": len(projects),
    }
    HEALTH_FILE.write_text(json.dumps(snapshot, indent=2))
    write_heartbeat(
        success=True,
        message=f"published {online_count} online / {ok_count} checks ok / {len(projects)} total"
    )

    try:
        git_commit_and_push()
    except Exception as e:
        print(f"[publisher] publish wrote files but push failed: {e}", file=sys.stderr)
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())
