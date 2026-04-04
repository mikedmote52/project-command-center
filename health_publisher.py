#!/usr/bin/env python3
"""
health_publisher.py — Project Command Center auto-health system

Reads (never writes) from other projects, publishes system_health.json to
the GitHub Pages repo so the dashboard can display real telemetry.

Usage:
  python3 health_publisher.py           # one-shot: collect + push
  python3 health_publisher.py --daemon  # run every 15 minutes
"""

import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

BASE_DIR = Path(__file__).parent
OUTPUT = BASE_DIR / "system_health.json"

# ── Helpers ──────────────────────────────────────────────────────────────────

def now_utc():
    return datetime.now(timezone.utc)

def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def minutes_since(ts):
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (now_utc() - dt).total_seconds() / 60
    except Exception:
        return None

def file_age_minutes(path):
    return (time.time() - path.stat().st_mtime) / 60

def empty_result(reason):
    return {"health": 0, "status": "offline", "summary": reason,
            "last_data": None, "warnings": [reason]}


# ── Per-system checks ─────────────────────────────────────────────────────────

def check_squeeze_prophet():
    """Parse mikes-trading-bot/data/health.json for live bot stats."""
    path = Path.home() / "Desktop/mikes-trading-bot/data/health.json"
    if not path.exists():
        return empty_result("health.json not found")
    try:
        data = json.loads(path.read_text())
    except Exception as e:
        return empty_result(f"JSON parse error: {e}")

    score = 0
    warnings = list(data.get("warnings", []))
    stats = data.get("stats", {})
    age_min = file_age_minutes(path)
    last_data = iso(datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc))

    # Running? (50 pts)
    bot_status = stats.get("status", "UNKNOWN")
    if bot_status == "ACTIVE":
        score += 50
        status = "running"
    else:
        status = "stopped"

    # Data freshness (25 pts)
    if age_min <= 30:
        score += 25
    elif age_min <= 120:
        score += 15
    elif age_min <= 480:
        score += 5
    if age_min > 60:
        warnings.append(f"health.json is {int(age_min)}m old")

    # Errors penalty (25 pts)
    errors = stats.get("errors_today", 0)
    if errors == 0:
        score += 25
    elif errors < 5:
        score += 15
    elif errors < 20:
        score += 5

    # Build summary
    equity = stats.get("equity", 0)
    num_pos = stats.get("num_positions", 0)
    trailing = stats.get("trailing", [])
    pnl_pct = stats.get("total_pnl_pct", 0)
    parts = [f"{num_pos} positions", f"equity ${equity/1000:.1f}K", f"P&L {pnl_pct:+.1f}%"]
    if trailing:
        parts.append(f"{len(trailing)} trailing winner{'s' if len(trailing) != 1 else ''}")

    return {"health": min(100, score), "status": status,
            "summary": ", ".join(parts), "last_data": last_data, "warnings": warnings}


def check_kalshi_intelligence():
    """Check Docker for Kalshi containers; fall back to unified_feed.json."""
    feed_path = Path.home() / "Desktop/intelligence-bridge/bridge-status/unified_feed.json"
    score = 0
    warnings = []
    status = "unknown"
    summary = ""
    last_data = None

    # Docker check
    DOCKER = (
        "/usr/local/bin/docker" if Path("/usr/local/bin/docker").exists()
        else "/opt/homebrew/bin/docker" if Path("/opt/homebrew/bin/docker").exists()
        else "docker"
    )
    containers_found = 0
    containers_running = 0
    try:
        proc = subprocess.run(
            [DOCKER, "ps", "--filter", "name=kalshi", "--format", "{{json .}}"],
            capture_output=True, text=True, timeout=10)
        if proc.returncode == 0:
            for line in proc.stdout.strip().splitlines():
                if line.strip():
                    try:
                        c = json.loads(line)
                        containers_found += 1
                        if "Up" in c.get("Status", ""):
                            containers_running += 1
                    except Exception:
                        pass
            if containers_found:
                score += 50 if containers_running == containers_found else (
                    25 if containers_running > 0 else 0)
                status = ("running" if containers_running == containers_found
                          else "degraded" if containers_running > 0 else "stopped")
                summary = f"{containers_running}/{containers_found} containers up"
                if containers_running < containers_found:
                    warnings.append(f"{containers_found - containers_running} containers down")
            else:
                status = "offline"
                summary = "No Kalshi containers running"
        else:
            warnings.append("docker ps failed")
    except FileNotFoundError:
        warnings.append("Docker not installed")
    except subprocess.TimeoutExpired:
        warnings.append("docker ps timed out")
    except Exception as e:
        warnings.append(f"Docker error: {e}")

    # Unified feed check for data freshness + signal counts
    try:
        feed = json.loads(feed_path.read_text())
        kal = feed.get("systems", {}).get("kalshi", {})
        pub_at = kal.get("published_at")
        if pub_at:
            last_data = pub_at
            mins = minutes_since(pub_at)
            if mins is not None:
                if mins <= 30:
                    score += 25
                elif mins <= 120:
                    score += 10
                if mins > 60:
                    warnings.append(f"Last Kalshi analysis {int(mins)}m ago")
            opps = kal.get("opportunities", 0)
            sigs = kal.get("sector_signals", 0)
            detail = f"{sigs} sector signals, {opps} opportunities"
            if summary:
                summary += f", {detail}"
            else:
                summary = detail
                if kal.get("status") == "live":
                    score = max(score, 40)
                    status = "degraded"
    except Exception:
        pass

    if not warnings:
        score += 25
    elif len(warnings) == 1:
        score += 10

    return {"health": min(100, score), "status": status,
            "summary": summary or "No data available", "last_data": last_data,
            "warnings": warnings}


def check_sentinelcompass():
    """Check compass.db modification time and row counts."""
    db_path = Path.home() / "Desktop/SentinelCompass/knowledge_base/compass.db"
    if not db_path.exists():
        return empty_result("compass.db not found")

    score = 0
    warnings = []
    age_min = file_age_minutes(db_path)
    last_data = iso(datetime.fromtimestamp(db_path.stat().st_mtime, tz=timezone.utc))

    # File freshness (25 pts)
    if age_min <= 60:
        score += 25
    elif age_min <= 480:
        score += 15
    elif age_min <= 1440:
        score += 5
    if age_min > 480:
        warnings.append(f"DB last updated {int(age_min/60)}h ago")

    # Row counts (50 pts)
    summary = ""
    status = "unknown"
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            counts = {}
            for tbl in ["entities", "statements", "claims", "market_signals",
                        "briefings", "pattern_alerts", "events"]:
                try:
                    row = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
                    counts[tbl] = row[0] if row else 0
                except Exception:
                    pass
            total = sum(counts.values())
            if total > 0:
                score += 50
                status = "running"
            else:
                status = "empty"
                warnings.append("Database is empty")
            parts = []
            for k, label in [("entities", "entities"), ("market_signals", "market signals"),
                              ("briefings", "briefings"), ("pattern_alerts", "patterns"),
                              ("events", "events")]:
                if counts.get(k, 0) > 0:
                    parts.append(f"{counts[k]} {label}")
            summary = ", ".join(parts) if parts else f"{total} records"
        finally:
            conn.close()
    except Exception as e:
        warnings.append(f"DB error: {e}")
        status = "error"
        summary = f"DB error: {e}"

    if not warnings:
        score += 25
    elif len(warnings) == 1:
        score += 10

    return {"health": min(100, score), "status": status, "summary": summary,
            "last_data": last_data, "warnings": warnings}


def check_intelligence_bridge():
    """Parse unified_feed.json for bridge health and signal counts."""
    path = Path.home() / "Desktop/intelligence-bridge/bridge-status/unified_feed.json"
    if not path.exists():
        return empty_result("unified_feed.json not found")
    try:
        data = json.loads(path.read_text())
    except Exception as e:
        return empty_result(f"JSON parse error: {e}")

    score = 0
    warnings = []
    generated_at = data.get("generated_at", "")
    last_data = generated_at or None

    # Feed freshness (50 pts)
    mins = minutes_since(generated_at) if generated_at else None
    if mins is not None:
        if mins <= 20:
            score += 50; status = "running"
        elif mins <= 60:
            score += 35; status = "running"
        elif mins <= 180:
            score += 20; status = "degraded"
            warnings.append(f"Feed last updated {int(mins)}m ago")
        else:
            score += 5; status = "stale"
            warnings.append(f"Feed last updated {int(mins/60)}h ago")
    else:
        status = "unknown"

    # Systems live (25 pts)
    systems = data.get("systems", {})
    live = [k for k, v in systems.items() if v.get("status") == "live"]
    total_sys = len(systems)
    if total_sys > 0:
        score += int(25 * len(live) / total_sys)
        offline = [k for k in systems if k not in live]
        if offline:
            warnings.append(f"Offline: {', '.join(offline)}")

    # Signal counts
    total_signals = 0
    for sd in systems.values():
        for key in ["sector_signals", "opportunities", "narrative_signals",
                    "pattern_alerts", "market_signals", "positions"]:
            total_signals += sd.get(key, 0)
    summary = f"{len(live)}/{total_sys} systems live, {total_signals} total signals"

    if not warnings:
        score += 25
    elif len(warnings) == 1:
        score += 10

    return {"health": min(100, score), "status": status, "summary": summary,
            "last_data": last_data, "warnings": warnings}


def check_mote_ops_landing():
    """HTTP check for the Mote Ops GitHub Pages site."""
    url = "https://mikedmote52.github.io/moteops-landing/"
    try:
        req = Request(url, headers={"User-Agent": "HealthPublisher/1.0"})
        t0 = time.time()
        with urlopen(req, timeout=15) as resp:
            elapsed = time.time() - t0
            code = resp.status
        last_data = iso(now_utc())
        ms = int(elapsed * 1000)
        summary = f"HTTP {code}, {ms}ms"
        if code == 200:
            if elapsed < 2:
                return {"health": 100, "status": "running", "summary": summary,
                        "last_data": last_data, "warnings": []}
            elif elapsed < 4:
                return {"health": 85, "status": "running", "summary": summary,
                        "last_data": last_data, "warnings": [f"Slow: {elapsed:.1f}s"]}
            else:
                return {"health": 65, "status": "degraded", "summary": summary,
                        "last_data": last_data, "warnings": [f"Very slow: {elapsed:.1f}s"]}
        else:
            return {"health": 30, "status": "degraded", "summary": summary,
                    "last_data": last_data, "warnings": [f"Unexpected status {code}"]}
    except URLError as e:
        return empty_result(f"Unreachable: {e.reason}")
    except Exception as e:
        return empty_result(str(e))


def check_project_command_center():
    """Always healthy — this script IS the health publisher."""
    return {"health": 100, "status": "running",
            "summary": "Health publisher active",
            "last_data": iso(now_utc()), "warnings": []}


# ── Orchestration ─────────────────────────────────────────────────────────────

# Keys match `subtitle` values in the dashboard DEFAULT_PROJECTS array
CHECKS = [
    ("mikes-trading-bot",      "Squeeze Prophet",        check_squeeze_prophet),
    ("claude-kalshi",          "Kalshi Intelligence",    check_kalshi_intelligence),
    ("sentinel-compass",       "SentinelCompass",        check_sentinelcompass),
    ("intelligence-bridge",    "Intelligence Bridge",    check_intelligence_bridge),
    ("side-gig-bot",           "Mote Ops Landing",       check_mote_ops_landing),
    ("project-command-center", "Project Command Center", check_project_command_center),
]


def collect_all():
    print(f"[{now_utc().strftime('%H:%M:%S')} UTC] Collecting health data...")
    projects = {}
    for subtitle_key, display_name, fn in CHECKS:
        print(f"  {display_name}...", end=" ", flush=True)
        try:
            result = fn()
            result["subtitle_match"] = subtitle_key
            result["project_id"] = subtitle_key
            result["collected_at"] = iso(now_utc())
            projects[subtitle_key] = result
            icon = "OK" if result["health"] >= 75 else ("WARN" if result["health"] >= 40 else "DOWN")
            print(f"[{icon}] {result['health']}% - {result['status']}")
        except Exception as e:
            print(f"[ERR] {e}")
            projects[subtitle_key] = {
                "health": 0, "status": "error",
                "summary": f"Check failed: {e}",
                "last_data": None, "warnings": [str(e)],
                "subtitle_match": subtitle_key, "project_id": subtitle_key,
                "collected_at": iso(now_utc())
            }
    return {"generated_at": iso(now_utc()), "projects": projects}


def publish(data):
    OUTPUT.write_text(json.dumps(data, indent=2))
    print(f"  Written -> {OUTPUT}")


def git_commit_push():
    try:
        subprocess.run(["git", "-C", str(BASE_DIR), "add", "system_health.json"],
                       check=True, capture_output=True)
        diff = subprocess.run(
            ["git", "-C", str(BASE_DIR), "diff", "--cached", "--quiet"],
            capture_output=True)
        if diff.returncode == 0:
            print("  No changes to commit.")
            return
        msg = f"auto: health update {now_utc().strftime('%Y-%m-%d %H:%M UTC')}"
        subprocess.run(["git", "-C", str(BASE_DIR), "commit", "-m", msg],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", str(BASE_DIR), "push"],
                       check=True, capture_output=True)
        print("  Pushed to GitHub.")
    except subprocess.CalledProcessError as e:
        err = e.stderr.decode() if e.stderr else str(e)
        print(f"  Git error: {err}")


def run_once():
    data = collect_all()
    publish(data)
    git_commit_push()
    print("Done.")


def run_daemon(interval_minutes=15):
    print(f"Daemon mode -- running every {interval_minutes} minutes. Ctrl-C to stop.")
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"Run error: {e}")
        next_run = datetime.fromtimestamp(
            time.time() + interval_minutes * 60).strftime("%H:%M:%S")
        print(f"Next run at {next_run}. Sleeping...")
        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    if "--daemon" in sys.argv:
        run_daemon()
    else:
        run_once()
