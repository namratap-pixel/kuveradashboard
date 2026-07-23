from flask import Flask, jsonify, render_template, request
import requests
import base64
from datetime import datetime, timedelta, date
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import os
import json
import logging
import atexit
import time

from data_sources import (
    get_attendance_month, get_leave_balance, get_holidays,
    get_roster_status_for_date,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

FRESHDESK_DOMAIN = "kuvera.freshdesk.com"
FRESHDESK_API_KEY = os.environ.get("FRESHDESK_API_KEY", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
IST = pytz.timezone("Asia/Kolkata")
FRT_SLA_HOURS = 12

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
ROUND_ROBIN_POLL_MINUTES = 5

cached_data       = {}
last_updated      = "Loading..."
incident_field_key = None
round_robin_state  = {}   # {date_str: {agent_name: {"on_minutes": int, "last_seen": iso, "currently_on": bool}}}


# ── Freshdesk plumbing ───────────────────────────────────────────────────

def fd_headers():
    encoded = base64.b64encode(f"{FRESHDESK_API_KEY}:X".encode()).decode()
    return {"Authorization": f"Basic {encoded}", "Content-Type": "application/json"}


def fd_get(endpoint, params=None):
    url = f"https://{FRESHDESK_DOMAIN}/api/v2/{endpoint}"
    while True:
        try:
            r = requests.get(url, headers=fd_headers(), params=params or {}, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            return r
        except Exception as e:
            logger.error(f"Request error {endpoint}: {e}")
            return None


def fetch_all_pages(endpoint, base_params):
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
        time.sleep(0.5)
    return results


def fetch_agents():
    r = fd_get("agents", {"per_page": 100})
    if r and r.status_code == 200:
        return {a["id"]: a["contact"]["name"] for a in r.json()}
    return {}


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


def fetch_csat():
    by_agent = {}
    r = fd_get("satisfaction_ratings", {"per_page": 100})
    if r is None or r.status_code != 200:
        return by_agent
    page = 1
    while True:
        r = fd_get("satisfaction_ratings", {"page": page, "per_page": 100})
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
        time.sleep(0.5)
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


def calc_frt(ticket, now_utc):
    created_at = ticket.get("created_at")
    if not created_at:
        return None, False, 0
    try:
        created = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
        responded_at = (ticket.get("stats") or {}).get("first_responded_at")
        if responded_at:
            responded = datetime.strptime(responded_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
            frt_hrs = (responded - created).total_seconds() / 3600
            breached = frt_hrs > FRT_SLA_HOURS
            over = round(max(0, frt_hrs - FRT_SLA_HOURS), 1)
            return round(frt_hrs, 1), breached, over
        else:
            age = (now_utc - created).total_seconds() / 3600
            breached = age > FRT_SLA_HOURS
            over = round(max(0, age - FRT_SLA_HOURS), 1)
            return None, breached, over
    except Exception:
        return None, False, 0


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
        # FRT
        "frt_within":        0,
        "frt_breached":      0,
        "_frt_over_sum":     0.0,
        "frt_breach_avg_hrs": None,
        "frt_score":         None,
        # ART (resolution)
        "resolution_rate":   0,
        "_res_sum":          0.0,
        "_res_n":            0,
        "art_hrs":           None,   # avg resolution time
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
    global cached_data, last_updated
    logger.info("Refreshing metrics...")

    if incident_field_key is None:
        detect_incident_type_field()

    agents_map = fetch_agents()
    target = {aid: name for aid, name in agents_map.items() if name in AGENT_NAMES}
    logger.info(f"Matched {len(target)}/{len(agents_map)} agents")

    now_utc  = datetime.now(pytz.utc)
    now_ist  = now_utc.astimezone(IST)
    today_ist = now_ist.strftime("%Y-%m-%d")

    since = (now_utc - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")
    all_tickets = fetch_all_pages("tickets", {"include": "stats", "updated_since": since})
    logger.info(f"Total tickets: {len(all_tickets)}")

    csat_map = fetch_csat()

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
        irt_cat  = classify_irt(t)

        is_today = False
        if created:
            try:
                c = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
                is_today = c.astimezone(IST).strftime("%Y-%m-%d") == today_ist
            except Exception:
                pass

        frt_hrs, frt_bad, frt_over = calc_frt(t, now_utc)

        for key in ("all", ch):
            m = metrics[name][key]

            if status in (STATUS_OPEN, STATUS_PENDING, STATUS_WAITING_CUSTOMER, STATUS_WAITING_THIRD_PARTY):
                if irt_cat == "IRT":
                    m["irt_count"] += 1
                elif irt_cat == "NON_IRT":
                    m["non_irt_count"] += 1
                else:
                    m["unknown_irt_count"] += 1

            if status == STATUS_OPEN:
                m["total_open"] += 1
                m["assigned_today" if is_today else "carry_forward"] += 1
                if frt_hrs is not None:
                    if frt_bad:
                        m["frt_breached"] += 1
                        m["_frt_over_sum"] += frt_over
                    else:
                        m["frt_within"] += 1
                elif frt_bad:
                    m["frt_breached"] += 1
                    m["_frt_over_sum"] += frt_over
                if priority >= 3:
                    m["urgent_high"] += 1
                if stats.get("resolved_at"):
                    m["reopened"] += 1

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
                        if ra_utc.astimezone(IST).strftime("%Y-%m-%d") == today_ist:
                            m["resolved_today"] += 1
                            if created:
                                c = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
                                m["_res_sum"] += (ra_utc - c).total_seconds() / 3600
                                m["_res_n"]   += 1
                    except Exception:
                        pass

    # Correct unresolved counts AND classify IRT/NON IRT from the same search API source
    # so that IRT + NON IRT always equals total_unresolved
    logger.info("Correcting unresolved counts and IRT/NON IRT via search API...")
    status_fields = [(2, "total_open"), (3, "pending"), (6, "waiting_customer"), (7, "waiting_third_party")]
    for agent_id, agent_name in target.items():
        m = metrics[agent_name]["all"]
        m["irt_count"] = 0
        m["non_irt_count"] = 0
        m["unknown_irt_count"] = 0

        for status_code, field in status_fields:
            page = 1
            while page <= 10:
                r = fd_get("search/tickets", {
                    "query": f'"status:{status_code} AND agent_id:{agent_id}"',
                    "page": page,
                })
                if r is None or r.status_code != 200:
                    break
                data = r.json()
                if page == 1:
                    count = data.get("total", 0)
                    m[field] = count
                    if field == "total_open":
                        m["carry_forward"] = max(0, count - m["assigned_today"])
                for t in data.get("results", []):
                    if classify_irt(t) == "IRT":
                        m["irt_count"] += 1
                    else:
                        m["non_irt_count"] += 1
                if len(data.get("results", [])) < 30:
                    break
                page += 1
                time.sleep(0.3)
            time.sleep(0.2)

        # if any agent has >300 tickets in a status the objects are truncated but
        # the count is exact — assign the gap to NON IRT so the totals still match
        classified = m["irt_count"] + m["non_irt_count"]
        total_unres = m["total_open"] + m["pending"] + m["waiting_customer"] + m["waiting_third_party"]
        if classified < total_unres:
            m["non_irt_count"] += total_unres - classified

    # Merge CSAT
    reversed_target = {v: k for k, v in target.items()}
    for name in AGENT_NAMES:
        aid = reversed_target.get(name)
        csat = csat_map.get(aid, {})
        if csat:
            for key in ("all", "Email", "Chat", "Phone"):
                m = metrics[name][key]
                m["csat_happy"]   = csat.get("happy", 0)
                m["csat_neutral"] = csat.get("neutral", 0)
                m["csat_bad"]     = csat.get("bad", 0)
                m["csat_total"]   = csat.get("total", 0)

    # Derived metrics + leaderboard score
    for name in AGENT_NAMES:
        for key in ("all", "Email", "Chat", "Phone"):
            m = metrics[name][key]

            m["total_unresolved"] = m["total_open"] + m["pending"] + m["waiting_customer"] + m["waiting_third_party"]

            frt_total = m["frt_within"] + m["frt_breached"]
            m["frt_score"] = round(m["frt_within"] / frt_total * 100) if frt_total > 0 else None
            m["frt_breach_avg_hrs"] = round(m["_frt_over_sum"] / m["frt_breached"], 1) \
                if m["frt_breached"] > 0 else None
            del m["_frt_over_sum"]

            total = m["total_open"] + m["resolved_today"]
            m["resolution_rate"] = round(m["resolved_today"] / total * 100) if total > 0 else 0
            m["art_hrs"] = round(m["_res_sum"] / m["_res_n"], 1) if m["_res_n"] > 0 else None
            del m["_res_sum"], m["_res_n"]

            if m["csat_total"] > 0:
                m["csat_score"] = round(m["csat_happy"] / m["csat_total"] * 100)

            ws, wt = 0.0, 0
            if m["frt_score"] is not None:
                ws += m["frt_score"] * 40; wt += 40
            if m["resolution_rate"] > 0:
                ws += m["resolution_rate"] * 35; wt += 35
            if m["csat_score"] is not None:
                ws += m["csat_score"] * 25; wt += 25
            m["overall_score"] = round(ws / wt) if wt > 0 else 0

    cached_data  = metrics
    last_updated = now_ist.strftime("%d %b %Y, %I:%M %p IST")
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


def poll_round_robin():
    """Runs every ROUND_ROBIN_POLL_MINUTES. Accumulates 'on' minutes per agent per day
    based on Freshdesk's live agent availability flag."""
    global round_robin_state
    try:
        agents_map = fetch_agents()
        avail = fetch_agent_availability()
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
    hdr = f"{'#':<3}{'Agent':<22}{'Score':>6}{'FRT%':>6}{'Res%':>6}{'CSAT%':>7}{'Open':>6}{'Unres':>6}"
    sep = "─" * 65
    rows = []
    agents_sorted = sorted(cached_data.items(),
                           key=lambda x: x[1]["all"]["overall_score"], reverse=True)
    for i, (name, data) in enumerate(agents_sorted, 1):
        m = data["all"]
        frt   = f"{m['frt_score']}%" if m["frt_score"] is not None else "—"
        csat  = f"{m['csat_score']}%" if m["csat_score"] is not None else "—"
        rows.append(
            f"{i:<3}{name:<22}{m['overall_score']:>5}%{frt:>6}{m['resolution_rate']:>5}%"
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
    cr = fd_get("satisfaction_ratings", {"per_page": 3})
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
                     "incident_type_field": incident_field_key})


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


@app.route("/health")
def health():
    return jsonify({"status": "ok", "last_updated": last_updated})


# ── Scheduler ──────────────────────────────────────────────────────────────
_load_rr_state()
compute_metrics()
poll_round_robin()

scheduler = BackgroundScheduler(timezone=IST)
scheduler.add_job(compute_metrics, "interval", hours=1, id="refresh")
scheduler.add_job(poll_round_robin, "interval", minutes=ROUND_ROBIN_POLL_MINUTES, id="rr_poll")
scheduler.add_job(post_slack_eod, "cron", hour=22, minute=0, timezone=IST, id="eod")
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
