from flask import Flask, jsonify, render_template, request
import requests
import concurrent.futures
import base64
from datetime import datetime, timedelta, date
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import os
import json
import logging
import atexit
import time
import threading
import socket

# OS-level socket timeout backstop — supplements per-request timeout.
socket.setdefaulttimeout(25)

# Module-level thread pool for fd_get. Wrapping each request in a Future means
# future.result(timeout=25) can abort even a hung DNS resolution, which no
# Python socket/requests timeout can interrupt.
_fd_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="fd_req")

from data_sources import (
    get_attendance_month, get_leave_balance, get_holidays,
    get_roster_status_for_date,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory log buffer so /logs endpoint can show what's happening
_log_buffer = []
_log_lock   = threading.Lock()

class _BufHandler(logging.Handler):
    def emit(self, record):
        with _log_lock:
            _log_buffer.append(f"{self.format(record)}")
            if len(_log_buffer) > 200:
                _log_buffer.pop(0)

_buf_handler = _BufHandler()
_buf_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
logging.getLogger().addHandler(_buf_handler)

app = Flask(__name__)

FRESHDESK_DOMAIN = "kuvera.freshdesk.com"
FRESHDESK_API_KEY = os.environ.get("FRESHDESK_API_KEY", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
IST = pytz.timezone("Asia/Kolkata")

# Override this in an env var if the Freshdesk custom field's internal key
# is known ahead of time (skips the auto-detect call). Internal keys usually
# look like "cf_incident_type".
INCIDENT_TYPE_FIELD_OVERRIDE = os.environ.get("INCIDENT_TYPE_FIELD", "")

AGENT_NAMES = [
    "Aparna More", "Aniket Gaste", "Ganesh Devkamble", "Vaishnavi Dongare",
    "Yogini Parmar", "Ashish Baral", "Ayush Sakpal", "Divija Sane",
    "Mayuri Patkar", "Shamith Sanil", "Vishal Gohel", "Aditya Sharma",
    "Rutuja Lad", "Shivam Nag", "Chaitali Thanekar", "Mohammed Waquar Vasta"
]

SOURCE_CHANNEL = {1: "Email", 2: "Email", 3: "Phone", 7: "Chat", 9: "Email", 10: "Email"}

# Freshdesk status codes
STATUS_OPEN     = 2
STATUS_PENDING  = 3
STATUS_RESOLVED = 4
STATUS_CLOSED   = 5
STATUS_WAITING_CUSTOMER    = 6
STATUS_WAITING_THIRD_PARTY = 7

ROUND_ROBIN_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "round_robin_log.json")
DASHBOARD_CACHE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_cache.json")
ROUND_ROBIN_POLL_MINUTES = 5

cached_data        = {}
last_updated       = "Loading..."
_computing         = False
_compute_error     = None
_compute_start_ts  = 0.0   # epoch seconds when current compute began
incident_field_key = None
round_robin_state  = {}   # {date_str: {agent_name: {"on_minutes": int, "last_seen": iso, "currently_on": bool}}}
_agents_cache      = {}   # {agent_id: name} — refreshed at most once per 2 hours
_agents_cache_ts   = 0.0  # epoch seconds


# ── Freshdesk plumbing ───────────────────────────────────────────────────

def fd_headers():
    encoded = base64.b64encode(f"{FRESHDESK_API_KEY}:X".encode()).decode()
    return {"Authorization": f"Basic {encoded}", "Content-Type": "application/json"}


def fd_get(endpoint, params=None, max_retries=0):
    url = f"https://{FRESHDESK_DOMAIN}/api/v2/{endpoint}"
    retries = 0
    while retries <= max_retries:
        try:
            # Wrap in a Future so future.result(timeout=25) can abort even
            # a hung DNS resolution, which socket/requests timeouts cannot.
            future = _fd_executor.submit(
                requests.get, url,
                headers=fd_headers(), params=params or {}, timeout=(5, 15)
            )
            r = future.result(timeout=25)
            if r.status_code == 429:
                wait = min(int(r.headers.get("Retry-After", 10)), 10)  # cap at 10s
                logger.warning(f"Rate limited on {endpoint} — waiting {wait}s (retry {retries+1}/{max_retries+1})")
                time.sleep(wait)
                retries += 1
                continue
            return r
        except concurrent.futures.TimeoutError:
            logger.error(f"fd_get timeout (25s) on {endpoint} — DNS or SSL hang")
            return None
        except Exception as e:
            logger.error(f"Request error {endpoint}: {e}")
            return None
    logger.warning(f"{endpoint}: gave up after rate limit")
    return None


def fetch_all_pages(endpoint, base_params, max_pages=None):
    results = []
    page = 1
    while True:
        r = fd_get(endpoint, {**base_params, "page": page, "per_page": 100})
        if r is None or r.status_code != 200:
            if r:
                logger.error(f"{endpoint} {r.status_code}: {r.text[:300]}")
            break
        data = r.json()
        if not data:
            break
        results.extend(data)
        if len(data) < 100:
            break
        page += 1
        if max_pages and page > max_pages:
            logger.warning(f"{endpoint}: hit max_pages={max_pages} cap ({len(results)} tickets)")
            break
        time.sleep(1.5)   # ~40 req/min — stays under Freshdesk's rate limit
    return results


def fetch_agents():
    logger.info("fetch_agents: calling fd_get agents")
    r = fd_get("agents", {"per_page": 100})
    logger.info(f"fetch_agents: got response {r.status_code if r else 'None'}")
    if r and r.status_code == 200:
        return {a["id"]: a["contact"]["name"] for a in r.json()}
    return {}


def fetch_agents_cached():
    """Return {agent_id: name}, refreshing at most once every 2 hours."""
    global _agents_cache, _agents_cache_ts
    if _agents_cache and (time.time() - _agents_cache_ts) < 7200:
        return dict(_agents_cache)
    result = fetch_agents()
    if result:
        _agents_cache = result
        _agents_cache_ts = time.time()
    return dict(_agents_cache) or {}


def fetch_agent_availability():
    """Returns {agent_id: bool_available} from the live agents list."""
    r = fd_get("agents", {"per_page": 100})
    out = {}
    if r and r.status_code == 200:
        for a in r.json():
            out[a["id"]] = bool(a.get("available"))
    return out


def detect_incident_type_field():
    """Auto-detects the Freshdesk custom field key for 'Incident Type'."""
    global incident_field_key
    if INCIDENT_TYPE_FIELD_OVERRIDE:
        incident_field_key = INCIDENT_TYPE_FIELD_OVERRIDE
        return incident_field_key
    r = fd_get("ticket_fields")
    if r and r.status_code == 200:
        for f in r.json():
            label = (f.get("label") or "").lower()
            name  = (f.get("name") or "").lower()
            if "incident" in label or "incident" in name:
                incident_field_key = f.get("name")
                logger.info(f"Detected incident-type field: {incident_field_key}")
                return incident_field_key
    logger.warning("Could not auto-detect Incident Type field — IRT/NON IRT split will show as unknown")
    incident_field_key = None
    return None


def classify_irt(ticket):
    if not incident_field_key:
        return "NON_IRT"
    cf = ticket.get("custom_fields") or {}
    val = cf.get(incident_field_key)
    if val is None or str(val).strip() == "":
        return "NON_IRT"
    if str(val).strip().upper().startswith("IRT"):
        return "IRT"
    return "NON_IRT"


def fetch_csat(since_days=30):
    by_agent = {}
    page = 1
    since_epoch = int((datetime.now(pytz.utc) - timedelta(days=since_days)).timestamp())
    while True:
        r = fd_get("surveys/satisfaction_ratings", {"since": since_epoch, "page": page, "per_page": 100})
        if r is None or r.status_code != 200:
            break
        data = r.json()
        if not data:
            break
        for item in data:
            aid = item.get("agent_id")
            if not aid:
                continue
            if aid not in by_agent:
                by_agent[aid] = {"happy": 0, "neutral": 0, "bad": 0, "total": 0}
            raw = item.get("ratings", {})
            val = None
            if isinstance(raw, dict):
                for v in raw.values():
                    if isinstance(v, (int, float)):
                        val = int(v)
                        break
            if val is None:
                continue
            by_agent[aid]["total"] += 1
            if val in (103, 5) or val >= 8:
                by_agent[aid]["happy"] += 1
            elif val in (102, 3, 4) or 5 <= val <= 7:
                by_agent[aid]["neutral"] += 1
            else:
                by_agent[aid]["bad"] += 1
        if len(data) < 100:
            break
        page += 1
        if page > 3:    # cap at 300 ratings — enough for per-agent averages
            break
        time.sleep(1.5)
    return by_agent


def get_channel(ticket):
    return SOURCE_CHANNEL.get(ticket.get("source", 0), "Email")


def pending_age_bucket(created_at, now_utc):
    try:
        c = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
        d = (now_utc - c).total_seconds() / 86400
        if d < 1:  return "d0_1"
        if d < 3:  return "d1_3"
        if d < 7:  return "d3_7"
        return "d7plus"
    except Exception:
        return "d7plus"


def business_hours_between(start_utc, end_utc):
    """Business hours (9 AM–8 PM IST, all days) between two UTC datetimes."""
    BIZ_START_H = 9
    BIZ_END_H   = 20
    if not start_utc or not end_utc or end_utc <= start_utc:
        return 0.0
    start_ist = start_utc.astimezone(IST).replace(tzinfo=None)
    end_ist   = end_utc.astimezone(IST).replace(tzinfo=None)
    total_secs = 0.0
    cursor = start_ist
    while cursor.date() <= end_ist.date():
        d = cursor.date()
        day_open  = datetime(d.year, d.month, d.day, BIZ_START_H)
        day_close = datetime(d.year, d.month, d.day, BIZ_END_H)
        seg_start = max(cursor, day_open)
        seg_end   = min(end_ist, day_close)
        if seg_end > seg_start:
            total_secs += (seg_end - seg_start).total_seconds()
        next_d = d + timedelta(days=1)
        cursor = datetime(next_d.year, next_d.month, next_d.day, BIZ_START_H)
        if cursor > end_ist:
            break
    return round(total_secs / 3600, 2)


def calc_biz_frt(ticket):
    """Business-hours FRT: assigned_at (or created_at) → first_responded_at."""
    stats = ticket.get("stats") or {}
    responded_at = stats.get("first_responded_at")
    if not responded_at:
        return None
    start_raw = stats.get("assigned_at") or ticket.get("created_at")
    if not start_raw:
        return None
    try:
        start = datetime.strptime(start_raw,    "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
        end   = datetime.strptime(responded_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
        return business_hours_between(start, end)
    except Exception:
        return None


def blank():
    return {
        # Productivity / workload
        "assigned_today":    0,
        "carry_forward":     0,
        "total_open":        0,
        "resolved_today":    0,
        "pending":           0,          # status 3 only
        "waiting_customer":  0,          # status 6
        "waiting_third_party": 0,        # status 7
        "total_unresolved":  0,          # open + pending + waiting_customer + waiting_third_party
        "irt_count":         0,
        "non_irt_count":     0,
        "unknown_irt_count": 0,
        "ageing":            {"d0_1": 0, "d1_3": 0, "d3_7": 0, "d7plus": 0},
        # FRT (business hours: assigned_at → first_responded_at)
        "_frt_today_sum":    0.0,
        "frt_today_count":   0,
        "frt_today_avg":     None,
        "_frt_3m_sum":       0.0,
        "frt_3m_count":      0,
        "frt_3m_avg":        None,
        # ART (business hours: created_at → resolved_at)
        "resolution_rate":   0,
        "_art_today_sum":    0.0,
        "art_today_count":   0,
        "art_today_avg":     None,
        "_art_3m_sum":       0.0,
        "art_3m_count":      0,
        "art_3m_avg":        None,
        # CSAT
        "csat_happy":        0,
        "csat_neutral":      0,
        "csat_bad":          0,
        "csat_total":        0,
        "csat_score":        None,
        # Quality
        "reopened":          0,
        "urgent_high":       0,
        "overall_score":     0,
    }


def compute_metrics():
    global cached_data, last_updated, _computing, _compute_error, _compute_start_ts
    _computing = True
    _compute_start_ts = time.time()
    _compute_error = None
    logger.info("Refreshing metrics...")

    if incident_field_key is None:
        detect_incident_type_field()

    agents_map = fetch_agents_cached()
    target = {aid: name for aid, name in agents_map.items() if name in AGENT_NAMES}
    logger.info(f"Matched {len(target)}/{len(agents_map)} agents")

    now_utc   = datetime.now(pytz.utc)
    now_ist   = now_utc.astimezone(IST)
    today_ist = now_ist.strftime("%Y-%m-%d")

    # Fetch tickets updated in last 14 days — capped at 3 pages (300 tickets)
    since = (now_utc - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    all_tickets = fetch_all_pages("tickets", {"include": "stats", "updated_since": since}, max_pages=3)
    logger.info(f"Tickets (14d): {len(all_tickets)}")

    # CSAT: Freshdesk ratings field is not populated via this endpoint — skip to save quota
    csat_map = {}

    metrics = {name: {"all": blank(), "Email": blank(), "Chat": blank(), "Phone": blank()}
               for name in AGENT_NAMES}

    for t in all_tickets:
        rid = t.get("responder_id")
        if rid not in target:
            continue

        name     = target[rid]
        ch       = get_channel(t)
        status   = t.get("status")
        stats    = t.get("stats") or {}
        priority = t.get("priority", 2)
        created  = t.get("created_at", "")
        biz_frt  = calc_biz_frt(t)

        is_today = False
        if created:
            try:
                c = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
                is_today = c.astimezone(IST).strftime("%Y-%m-%d") == today_ist
            except Exception:
                pass

        for key in ("all", ch):
            m = metrics[name][key]

            # FRT: any ticket that has a first response in our window
            if biz_frt is not None:
                try:
                    resp_str = stats.get("first_responded_at", "")
                    if resp_str:
                        resp_ist = (datetime.strptime(resp_str, "%Y-%m-%dT%H:%M:%SZ")
                                    .replace(tzinfo=pytz.utc).astimezone(IST).strftime("%Y-%m-%d"))
                        m["_frt_3m_sum"]  += biz_frt
                        m["frt_3m_count"] += 1
                        if resp_ist == today_ist:
                            m["_frt_today_sum"]  += biz_frt
                            m["frt_today_count"] += 1
                except Exception:
                    pass

            if status == STATUS_OPEN:
                m["total_open"] += 1
                m["assigned_today" if is_today else "carry_forward"] += 1
                if priority >= 3:
                    m["urgent_high"] += 1
                if stats.get("resolved_at"):
                    m["reopened"] += 1
                irt = classify_irt(t)
                if irt == "IRT":
                    m["irt_count"] += 1
                else:
                    m["non_irt_count"] += 1

            elif status == STATUS_PENDING:
                m["pending"] += 1
                m["ageing"][pending_age_bucket(created, now_utc)] += 1

            elif status == STATUS_WAITING_CUSTOMER:
                m["waiting_customer"] += 1

            elif status == STATUS_WAITING_THIRD_PARTY:
                m["waiting_third_party"] += 1

            elif status == STATUS_RESOLVED:
                ra = stats.get("resolved_at")
                if ra:
                    try:
                        ra_utc = datetime.strptime(ra, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
                        ra_ist = ra_utc.astimezone(IST).strftime("%Y-%m-%d")
                        if ra_ist == today_ist:
                            m["resolved_today"] += 1
                        if created:
                            c = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
                            biz_art = business_hours_between(c, ra_utc)
                            m["_art_3m_sum"]  += biz_art
                            m["art_3m_count"] += 1
                            if ra_ist == today_ist:
                                m["_art_today_sum"]  += biz_art
                                m["art_today_count"] += 1
                    except Exception:
                        pass

    # ── Merge CSAT ────────────────────────────────────────────────────────
    reversed_target = {v: k for k, v in target.items()}
    for name in AGENT_NAMES:
        aid  = reversed_target.get(name)
        csat = csat_map.get(aid, {})
        if csat:
            for key in ("all", "Email", "Chat", "Phone"):
                m = metrics[name][key]
                m["csat_happy"]   = csat.get("happy", 0)
                m["csat_neutral"] = csat.get("neutral", 0)
                m["csat_bad"]     = csat.get("bad", 0)
                m["csat_total"]   = csat.get("total", 0)

    # ── Derived metrics ───────────────────────────────────────────────────
    for name in AGENT_NAMES:
        for key in ("all", "Email", "Chat", "Phone"):
            m = metrics[name][key]

            m["total_unresolved"] = (m["total_open"] + m["pending"]
                                     + m["waiting_customer"] + m["waiting_third_party"])

            m["frt_today_avg"] = round(m["_frt_today_sum"] / m["frt_today_count"], 1) \
                if m["frt_today_count"] > 0 else None
            m["frt_3m_avg"] = round(m["_frt_3m_sum"] / m["frt_3m_count"], 1) \
                if m["frt_3m_count"] > 0 else None
            del m["_frt_today_sum"], m["_frt_3m_sum"]

            total = m["total_open"] + m["resolved_today"]
            m["resolution_rate"] = round(m["resolved_today"] / total * 100) if total > 0 else 0
            m["art_today_avg"] = round(m["_art_today_sum"] / m["art_today_count"], 1) \
                if m["art_today_count"] > 0 else None
            m["art_3m_avg"] = round(m["_art_3m_sum"] / m["art_3m_count"], 1) \
                if m["art_3m_count"] > 0 else None
            del m["_art_today_sum"], m["_art_3m_sum"]

            if m["csat_total"] > 0:
                m["csat_score"] = round(m["csat_happy"] / m["csat_total"] * 100)

            ws, wt = 0.0, 0
            if m["resolution_rate"] > 0:
                ws += m["resolution_rate"] * 60; wt += 60
            if m["csat_score"] is not None:
                ws += m["csat_score"] * 40; wt += 40
            m["overall_score"] = round(ws / wt) if wt > 0 else 0

    cached_data  = metrics
    last_updated = now_ist.strftime("%d %b %Y, %I:%M %p IST")
    _computing   = False
    _save_dashboard_cache()
    logger.info(f"Done: {last_updated}")


# ── Round-robin on/off tracking ─────────────────────────────────────────

def _load_rr_state():
    global round_robin_state
    if os.path.exists(ROUND_ROBIN_LOG_PATH):
        try:
            with open(ROUND_ROBIN_LOG_PATH) as f:
                round_robin_state = json.load(f)
        except Exception:
            round_robin_state = {}


def _save_rr_state():
    try:
        with open(ROUND_ROBIN_LOG_PATH, "w") as f:
            json.dump(round_robin_state, f)
    except Exception as e:
        logger.error(f"Could not persist round-robin log: {e}")


def _save_dashboard_cache():
    try:
        with open(DASHBOARD_CACHE_PATH, "w") as f:
            json.dump({"data": cached_data, "last_updated": last_updated}, f)
    except Exception as e:
        logger.warning(f"Could not save dashboard cache: {e}")


def _load_dashboard_cache():
    global cached_data, last_updated
    try:
        if os.path.exists(DASHBOARD_CACHE_PATH):
            with open(DASHBOARD_CACHE_PATH) as f:
                d = json.load(f)
            if d.get("data"):
                cached_data  = d["data"]
                last_updated = d.get("last_updated", "Cached")
                logger.info(f"Loaded dashboard cache: {last_updated}")
    except Exception as e:
        logger.warning(f"Could not load dashboard cache: {e}")


def poll_round_robin():
    """Runs every ROUND_ROBIN_POLL_MINUTES. Accumulates 'on' minutes per agent per day
    based on Freshdesk's live agent availability flag."""
    global round_robin_state, _agents_cache, _agents_cache_ts
    try:
        r = fd_get("agents", {"per_page": 100})
        if not r or r.status_code != 200:
            logger.error(f"Round-robin poll: agents fetch failed")
            return
        all_agents = r.json()
        agents_map = {a["id"]: a["contact"]["name"] for a in all_agents}
        avail      = {a["id"]: bool(a.get("available")) for a in all_agents}
        # Refresh the shared cache while we're at it
        _agents_cache    = agents_map
        _agents_cache_ts = time.time()
    except Exception as e:
        logger.error(f"Round-robin poll failed: {e}")
        return

    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    now_iso = datetime.now(IST).isoformat()
    day_state = round_robin_state.setdefault(today_str, {})

    for aid, name in agents_map.items():
        if name not in AGENT_NAMES:
            continue
        is_on = avail.get(aid, False)
        rec = day_state.setdefault(name, {"on_minutes": 0, "last_seen": now_iso, "currently_on": is_on})
        if is_on:
            # Credit this poll interval (capped so a missed/late poll can't inflate the total)
            rec["on_minutes"] = rec.get("on_minutes", 0) + min(ROUND_ROBIN_POLL_MINUTES, 30)
        rec["currently_on"] = is_on
        rec["last_seen"] = now_iso

    # keep only the last 40 days of history
    for k in list(round_robin_state.keys()):
        try:
            if (date.today() - date.fromisoformat(k)).days > 40:
                del round_robin_state[k]
        except Exception:
            pass

    _save_rr_state()


def get_round_robin_today():
    """Merges live polled on/off minutes with today's roster (week-off overrides)."""
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    day_state = round_robin_state.get(today_str, {})
    roster = get_roster_status_for_date(today)

    out = []
    for name in AGENT_NAMES:
        roster_status = roster.get(name)
        rec = day_state.get(name, {"on_minutes": 0, "currently_on": False})
        minutes = rec.get("on_minutes", 0)
        hours = round(minutes / 60, 1)

        if roster_status == "WO":
            status_label = "Off — Week Off"
        elif roster_status and roster_status not in ("SHIFT",):
            status_label = f"Off — {roster_status}"
        elif rec.get("currently_on"):
            status_label = f"On — {hours}h today"
        elif minutes > 0:
            status_label = f"Off now — {hours}h on earlier today"
        else:
            status_label = "Off"

        out.append({
            "name": name,
            "roster_status": roster_status or "Unknown",
            "on_minutes_today": minutes,
            "on_hours_today": hours,
            "currently_on": bool(rec.get("currently_on")) and roster_status != "WO",
            "status_label": status_label,
        })
    return out


# ── Slack EOD ─────────────────────────────────────────────────────────────

def post_slack_eod():
    if not cached_data or not SLACK_WEBHOOK_URL:
        logger.warning("Skipping EOD report")
        return
    now_ist_s = datetime.now(IST).strftime("%d %b %Y")
    hdr = f"{'#':<3}{'Agent':<22}{'Score':>6}{'FRT(h)':>7}{'Res%':>6}{'CSAT%':>7}{'Open':>6}{'Unres':>6}"
    sep = "─" * 67
    rows = []
    agents_sorted = sorted(cached_data.items(),
                           key=lambda x: x[1]["all"]["overall_score"], reverse=True)
    for i, (name, data) in enumerate(agents_sorted, 1):
        m = data["all"]
        frt   = f"{m['frt_today_avg']}h" if m["frt_today_avg"] is not None else "—"
        csat  = f"{m['csat_score']}%" if m["csat_score"] is not None else "—"
        rows.append(
            f"{i:<3}{name:<22}{m['overall_score']:>5}%{frt:>7}{m['resolution_rate']:>5}%"
            f"{csat:>7}{m['total_open']:>6}{m['total_unresolved']:>6}"
        )
    text = "\n".join([
        f"*📊 Kuvera Ops Leaderboard | {now_ist_s}*", "",
        "```", hdr, sep, *rows, "```"
    ])
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        logger.info(f"Slack EOD: {r.status_code}")
    except Exception as e:
        logger.error(f"Slack error: {e}")


# ── Routes ────────────────────────────────────────────────────────────────

@app.route("/debug")
def debug():
    r  = fd_get("tickets",  {"per_page": 3})
    ar = fd_get("agents",   {"per_page": 3})
    cr = fd_get("surveys/satisfaction_ratings", {"per_page": 3})
    return jsonify({
        "tickets_status": r.status_code  if r  else "error",
        "tickets":        r.json()[:2]   if r and r.status_code == 200 else (r.text[:300] if r else None),
        "agents_status":  ar.status_code if ar else "error",
        "agents":         ar.json()[:2]  if ar and ar.status_code == 200 else None,
        "csat_status":    cr.status_code if cr else "error",
        "csat_sample":    cr.json()[:2]  if cr and cr.status_code == 200 else (cr.text[:200] if cr else None),
        "incident_type_field": incident_field_key,
    })


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/metrics")
def metrics_api():
    return jsonify({"data": cached_data, "last_updated": last_updated,
                     "incident_type_field": incident_field_key,
                     "computing": _computing,
                     "compute_error": _compute_error})


@app.route("/api/attendance")
def attendance_api():
    year = int(request.args.get("year", date.today().year))
    month = int(request.args.get("month", date.today().month))
    return jsonify(get_attendance_month(year, month))


@app.route("/api/leave-balance")
def leave_balance_api():
    return jsonify({"agents": get_leave_balance(), "holidays": get_holidays()})


@app.route("/api/round-robin")
def round_robin_api():
    return jsonify({"agents": get_round_robin_today(), "date": date.today().isoformat()})


@app.route("/force-refresh")
def force_refresh():
    if _computing:
        return jsonify({"status": "in_progress", "message": "sync already running"})
    try:
        compute_metrics()
        return jsonify({"status": "ok", "last_updated": last_updated, "agents": len(cached_data)})
    except Exception as e:
        logger.error(f"force-refresh failed: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/health")
def health():
    elapsed = round(time.time() - _compute_start_ts, 1) if _computing and _compute_start_ts > 0 else None
    return jsonify({
        "status": "ok",
        "last_updated": last_updated,
        "computing": _computing,
        "compute_error": _compute_error,
        "compute_elapsed_s": elapsed,
        "agents_loaded": len(cached_data),
        "api_key_set": bool(FRESHDESK_API_KEY),
    })


@app.route("/logs")
def logs():
    with _log_lock:
        lines = list(_log_buffer)
    return "<pre style='font-family:monospace;font-size:13px'>" + "\n".join(lines) + "</pre>", 200


# ── Scheduler ──────────────────────────────────────────────────────────────
_load_rr_state()
_load_dashboard_cache()

def _bg_compute():
    try:
        compute_metrics()
    except Exception as e:
        global _computing, _compute_error
        _computing = False
        _compute_error = str(e)
        logger.error(f"compute_metrics crashed: {e}", exc_info=True)


def _scheduled_compute():
    if _computing:
        logger.warning("Skipping scheduled compute — previous still running")
        return
    _bg_compute()


def _compute_watchdog():
    """Runs every minute; resets _computing if stuck for >3 minutes."""
    global _computing, _compute_error, _compute_start_ts
    if _computing and _compute_start_ts > 0 and (time.time() - _compute_start_ts) > 180:
        elapsed = int(time.time() - _compute_start_ts)
        _computing = False
        _compute_error = f"Watchdog: timed out after {elapsed}s — check /logs"
        logger.error(f"Watchdog fired — compute ran {elapsed}s, forcing _computing=False")


def _startup_compute():
    logger.info("Startup compute beginning")
    _bg_compute()


threading.Thread(target=_startup_compute, daemon=True).start()
# Do NOT call poll_round_robin() synchronously at startup — it would race with
# the compute thread for Freshdesk API quota and cause extra rate-limit sleeps.
# The scheduler fires the first RR poll at T+10min (after compute is done).
_rr_first_run = datetime.now(IST) + timedelta(minutes=10)

scheduler = BackgroundScheduler(timezone=IST)
scheduler.add_job(_scheduled_compute, "interval", hours=1, id="refresh")
scheduler.add_job(poll_round_robin, "interval", minutes=ROUND_ROBIN_POLL_MINUTES, id="rr_poll",
                  next_run_time=_rr_first_run)
scheduler.add_job(_compute_watchdog, "interval", minutes=1, id="watchdog")
scheduler.add_job(post_slack_eod, "cron", hour=22, minute=0, timezone=IST, id="eod")
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
