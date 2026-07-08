from flask import Flask, jsonify, render_template
import requests
import base64
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import os
import logging
import atexit
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

FRESHDESK_DOMAIN = "kuvera.freshdesk.com"
FRESHDESK_API_KEY = os.environ.get("FRESHDESK_API_KEY", "sr2CuFC40JT0w0KOj1kw")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
IST = pytz.timezone("Asia/Kolkata")
FRT_SLA_HOURS = 12

AGENT_NAMES = [
    "Aparna More", "Aniket Gaste", "Ganesh Devkamble", "Vaishnavi Dongare",
    "Yogini Parmar", "Ashish Baral", "Ayush Sakpal", "Divija Sane",
    "Mayuri Patkar", "Shamith Sanil", "Vishal Gohel", "Aditya Sharma",
    "Rutuja Lad", "Shivam Nag", "Chaitali Thanekar", "Mohammed Waquar Vasta"
]

SOURCE_CHANNEL = {1: "Email", 2: "Email", 3: "Phone", 7: "Chat", 9: "Email", 10: "Email"}

# Freshdesk status codes
STATUS_OPEN     = 2
STATUS_PENDING  = 3   # Pending
STATUS_RESOLVED = 4
STATUS_CLOSED   = 5
STATUS_WAITING_CUSTOMER   = 6   # Waiting on Customer
STATUS_WAITING_THIRD_PARTY = 7  # Waiting on Third Party

PENDING_STATUSES = {STATUS_PENDING, STATUS_WAITING_CUSTOMER, STATUS_WAITING_THIRD_PARTY}

cached_data  = {}
last_updated = "Loading..."


def fd_headers():
    encoded = base64.b64encode(f"{FRESHDESK_API_KEY}:X".encode()).decode()
    return {"Authorization": f"Basic {encoded}", "Content-Type": "application/json"}


def fd_get(endpoint, params=None):
    """Single page GET with rate-limit retry."""
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
    """Paginate through all pages of an endpoint."""
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
        logger.info(f"  {endpoint} page {page}: {len(data)} items")
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


def fetch_csat():
    """Returns {agent_id: {happy, neutral, bad, total}} for last 30 days."""
    by_agent = {}
    r = fd_get("satisfaction_ratings", {"per_page": 100})
    if r is None:
        return by_agent
    if r.status_code == 403:
        logger.warning("CSAT: no access (403) — feature may not be enabled")
        return by_agent
    if r.status_code != 200:
        logger.error(f"CSAT {r.status_code}: {r.text[:200]}")
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

            # Freshdesk CSAT: ratings dict may use 101/102/103 or 1-5
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
    """Returns (frt_hours_or_None, is_breached, hrs_over_sla)."""
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
        # Workload
        "assigned_today":    0,
        "carry_forward":     0,
        "total_open":        0,
        # FRT
        "frt_within":        0,
        "frt_breached":      0,
        "_frt_over_sum":     0.0,
        "frt_breach_avg_hrs": None,
        "frt_score":         None,
        # Resolution
        "resolved_today":    0,
        "resolution_rate":   0,
        "_res_sum":          0.0,
        "_res_n":            0,
        "avg_resolution_hrs": None,
        # Pending (open + waiting on customer/third party)
        "pending":           0,
        "ageing":            {"d0_1": 0, "d1_3": 0, "d3_7": 0, "d7plus": 0},
        # CSAT
        "csat_happy":        0,
        "csat_neutral":      0,
        "csat_bad":          0,
        "csat_total":        0,
        "csat_score":        None,
        # Quality
        "reopened":          0,
        "urgent_high":       0,
        # Leaderboard
        "overall_score":     0,
    }


def compute_metrics():
    global cached_data, last_updated
    logger.info("Refreshing metrics...")

    agents_map = fetch_agents()
    target = {aid: name for aid, name in agents_map.items() if name in AGENT_NAMES}
    logger.info(f"Matched {len(target)}/{len(agents_map)} agents")

    now_utc  = datetime.now(pytz.utc)
    now_ist  = now_utc.astimezone(IST)
    today_ist = now_ist.strftime("%Y-%m-%d")

    since = (now_utc - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")
    all_tickets = fetch_all_pages("tickets", {"include": "stats", "updated_since": since})
    logger.info(f"Total tickets: {len(all_tickets)}")

    # CSAT ratings (keyed by agent_id)
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

            if status == STATUS_OPEN:
                m["total_open"] += 1
                m["assigned_today" if is_today else "carry_forward"] += 1
                if frt_hrs is not None:
                    if frt_bad:
                        m["frt_breached"] += 1
                        m["_frt_over_sum"] += frt_over
                    else:
                        m["frt_within"] += 1
                elif frt_bad:   # no response yet, past SLA
                    m["frt_breached"] += 1
                    m["_frt_over_sum"] += frt_over
                if priority >= 3:
                    m["urgent_high"] += 1
                if stats.get("resolved_at"):
                    m["reopened"] += 1

            elif status in PENDING_STATUSES:
                m["pending"] += 1
                m["ageing"][pending_age_bucket(created, now_utc)] += 1
                if frt_hrs is not None:
                    if frt_bad:
                        m["frt_breached"] += 1
                        m["_frt_over_sum"] += frt_over
                    else:
                        m["frt_within"] += 1

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

    # Correct total_open using search API (no date cutoff — catches all open tickets)
    logger.info("Correcting open ticket counts via search API...")
    for agent_id, agent_name in target.items():
        r = fd_get("search/tickets", {"query": f'"status:2 AND agent_id:{agent_id}"'})
        if r and r.status_code == 200:
            true_open = r.json().get("total", 0)
            for ch in ("all", "Email", "Chat", "Phone"):
                m = metrics[agent_name][ch]
                if ch == "all":
                    m["total_open"] = true_open
                    m["carry_forward"] = max(0, true_open - m["assigned_today"])
        time.sleep(0.5)

    # Merge CSAT into metrics
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

            # FRT score
            frt_total = m["frt_within"] + m["frt_breached"]
            m["frt_score"] = round(m["frt_within"] / frt_total * 100) if frt_total > 0 else None
            m["frt_breach_avg_hrs"] = round(m["_frt_over_sum"] / m["frt_breached"], 1) \
                if m["frt_breached"] > 0 else None
            del m["_frt_over_sum"]

            # Resolution
            total = m["total_open"] + m["resolved_today"]
            m["resolution_rate"] = round(m["resolved_today"] / total * 100) if total > 0 else 0
            m["avg_resolution_hrs"] = round(m["_res_sum"] / m["_res_n"], 1) if m["_res_n"] > 0 else None
            del m["_res_sum"], m["_res_n"]

            # CSAT
            if m["csat_total"] > 0:
                m["csat_score"] = round(m["csat_happy"] / m["csat_total"] * 100)

            # Overall leaderboard score (weighted)
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


def post_slack_eod():
    if not cached_data or not SLACK_WEBHOOK_URL:
        logger.warning("Skipping EOD report")
        return
    now_ist = datetime.now(IST).strftime("%d %b %Y")
    hdr = f"{'#':<3}{'Agent':<22}{'Score':>6}{'FRT%':>6}{'Res%':>6}{'CSAT%':>7}{'Open':>6}{'Pend':>6}"
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
            f"{csat:>7}{m['total_open']:>6}{m['pending']:>6}"
        )
    text = "\n".join([
        f"*📊 Kuvera Ops Leaderboard | {now_ist}*", "",
        "```", hdr, sep, *rows, "```"
    ])
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        logger.info(f"Slack EOD: {r.status_code}")
    except Exception as e:
        logger.error(f"Slack error: {e}")


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
    })


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/metrics")
def metrics_api():
    return jsonify({"data": cached_data, "last_updated": last_updated})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "last_updated": last_updated})


# ── Scheduler ──────────────────────────────────────────────────────────────────
compute_metrics()

scheduler = BackgroundScheduler(timezone=IST)
scheduler.add_job(compute_metrics, "interval", hours=1, id="refresh")
scheduler.add_job(post_slack_eod, "cron", hour=22, minute=0, timezone=IST, id="eod")
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
