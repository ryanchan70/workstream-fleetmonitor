"""
The work that the old `while True:` loop used to do, reshaped into two
idempotent operations that any invocation can safely run:

  poll()     — one iteration of the fleet loop, driven by open browser tabs
  backfill() — full session history sweep, on first boot and then hourly

Both take a Redis lock, so several tabs polling at once cannot double-count.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import core as C
from . import fleet
from . import redis_state as R

# Budget for fanning out per-device session fetches. Vercel kills the
# invocation at maxDuration, so this stays well inside it.
SESSION_FETCH_BUDGET = 20.0
SESSION_FETCH_WORKERS = 8


def _log(*lines):
    try:
        R.log_push([f"[{C.ts()}] {l}" for l in lines])
    except R.RedisUnavailable:
        pass


# ── Poll ──────────────────────────────────────────────────────────────────
def poll(force: bool = False) -> dict:
    """One iteration of the fleet loop.

    Returns a summary dict. Safe to call as often as tabs fire it: if another
    invocation holds the lock this returns immediately and the caller just
    reads the cached state.
    """
    if not R.acquire_lock("poll", ttl=30):
        return {"skipped": "locked"}

    try:
        devices = fleet.fleet_status()
        if not devices:
            return {"skipped": "no devices"}

        today = C.get_date_str()
        locations = R.hgetall_json("locations")
        prev_status = R.jget("prev_status", {}) or {}

        # Evaluate with NO debounce, so `alerts` is the set of conditions that
        # are genuinely true right now.
        #
        # These are two different questions and conflating them is a bug:
        #   "is the condition true?"  -> drives CRITICAL/RESOLVED transitions
        #   "should we notify again?" -> the 15-minute quiet period
        # Debouncing the evaluation itself made a still-failing rig look like
        # it had recovered, so the log oscillated CRITICAL -> RESOLVED ->
        # CRITICAL every 15 minutes while nothing actually changed.
        rigs, alerts = C.evaluate_rigs(
            devices,
            locations=locations,
            should_alert=None,
            prev_status=prev_status,
        )

        active = R.jget("active_alerts", {}) or {}
        current = {f"{a['hostname']}|{a['kind']}": a for a in alerts}

        # Transitions fire exactly once per incident.
        for k, a in current.items():
            if k not in active:
                _log(f"CRITICAL {a['rig']}: {a['message']}")
        for k, a in list(active.items()):
            if k not in current:
                _log(f"RESOLVED {a['rig']}: cleared — {a.get('message','')}")
        R.jset("active_alerts", current)

        # The quiet period applies only to re-notifying. A newly-appeared
        # condition always notifies; an ongoing one stays silent until the
        # window lapses. The alert itself is still reported either way, so the
        # rig tile keeps showing the warning.
        for k, a in current.items():
            a["notify"] = (k not in active) or R.should_alert(
                a["hostname"], a["kind"], C.ALERT_DEBOUNCE_SEC)

        # Frame health snapshot.
        readings = [r["frame_health_pct"] for r in rigs
                    if r.get("frame_health_pct") is not None and r.get("online")]
        if readings:
            R.health_push({
                "t": int(time.time() * 1000),
                "avg": round(sum(readings) / len(readings), 2),
                "min": round(min(readings), 2),
            })

        # Idempotent observed-time tracker. Preserved verbatim in spirit from
        # the original: max() means re-observing the same recording can never
        # add time twice; a segment is only banked when the operator changes,
        # the counter resets, or the rig cleanly stops.
        observed = R.hgetall_json("observed")
        new_status = {}
        finished = []
        for d in devices:
            host = d.get("hostname") or ""
            if not host:
                continue
            status = C.get_status(d)
            label = C.device_label(d)
            new_status[host] = status

            st = observed.get(host)
            if not isinstance(st, dict) or st.get("date") != today:
                st = {"date": today, "banked_s": 0.0, "cur_dur": 0.0,
                      "cur_op": "", "label": label}
            st["label"] = label

            if status == "recording":
                op = C.clean_str(d.get("operator"))
                dur = float(d.get("recording_duration_s") or 0)
                if st["cur_op"] and (op != st["cur_op"] or dur < st["cur_dur"] - 30):
                    st["banked_s"] += st["cur_dur"]
                    st["cur_dur"] = dur
                    st["cur_op"] = op
                else:
                    st["cur_op"] = op
                    st["cur_dur"] = max(st["cur_dur"], dur)
            elif status == "offline":
                pass    # freeze; reconnecting mid-recording just raises cur_dur
            else:
                if st["cur_dur"] > 0:
                    # Clean stop -> bank it and record the completed task.
                    finished.append({
                        "hostname": host,
                        "label": label,
                        "operator": st.get("cur_op") or "Unknown",
                        "task": C.clean_str(d.get("task")),
                        "duration_s": st["cur_dur"],
                        "start_time": int(time.time() - st["cur_dur"]),
                    })
                    st["banked_s"] += st["cur_dur"]
                    st["cur_dur"] = 0.0
                    st["cur_op"] = ""
            observed[host] = st

        R.hset_many_json("observed", observed)
        R.jset("prev_status", new_status)

        for f in finished:
            sid = f"live|{f['hostname']}|{f['start_time']}|{int(f['duration_s'])}"
            R.hset_json("tasks:" + f["hostname"], sid, {
                "label": f["label"], "operator": f["operator"], "task": f["task"],
                "duration_s": f["duration_s"], "start_time": f["start_time"],
            })
            _log(f"CHANGE {f['label']}  STOPPED | Op: {f['operator']} | "
                 f"{C.format_time(f['duration_s'])}")

        # Cache rig cards for /devices.
        R.jset("rigs", {"rigs": rigs, "alerts": alerts,
                        "updated": int(time.time() * 1000)}, ttl=300)

        _rebuild_leaderboard(devices, today, observed)

        return {"rigs": len(rigs), "alerts": len(alerts), "finished": len(finished)}
    finally:
        R.release_lock("poll")


def _rebuild_leaderboard(devices, today, observed):
    """Today's per-Pi / per-operator totals from the API session lists, topped
    up by our own observed time where the API has not caught up yet."""
    by_pi, by_op = {}, {}
    status = {d.get("hostname"): C.get_status(d) for d in devices if d.get("hostname")}
    labels = {d.get("hostname"): C.device_label(d) for d in devices if d.get("hostname")}

    cached = R.hgetall_json("today_sessions")
    results = {}
    deadline = time.time() + SESSION_FETCH_BUDGET

    targets = [h for h, s in status.items() if s != "offline"]
    if targets:
        with ThreadPoolExecutor(max_workers=SESSION_FETCH_WORKERS) as ex:
            futs = {ex.submit(fleet.device_sessions, h): h for h in targets}
            for fut in as_completed(futs, timeout=max(1.0, deadline - time.time())):
                h = futs[fut]
                try:
                    groups = [g for g in (fut.result() or [])
                              if C.session_is_today(g, today)]
                    results[h] = groups
                except Exception:
                    pass
                if time.time() > deadline:
                    break

    # Devices we could not reach keep their last known sessions rather than
    # dropping to zero and collapsing the operator totals.
    for h in status:
        if h not in results and h in cached:
            results[h] = cached[h]
    if results:
        R.hset_many_json("today_sessions", results)

    seen = set()
    for h, groups in results.items():
        label = labels.get(h, h)
        for rec in groups:
            sid = rec.get("id") or rec.get("session_uuid") or f"{label}|{rec.get('name')}"
            if sid in seen:
                continue
            seen.add(sid)
            dur = float(rec.get("duration_s") or 0)
            op = C.clean_str(rec.get("operator"))
            by_pi[label] = by_pi.get(label, 0.0) + dur
            by_op[op] = by_op.get(op, 0.0) + dur

    # Fold in self-tracked time using max(), never addition, so a host can
    # never be counted from both its API total and its observed total.
    for host, st in observed.items():
        if not isinstance(st, dict) or st.get("date") != today:
            continue
        label = labels.get(host, st.get("label", host))
        live_now = status.get(host) == "recording"
        obs = st.get("banked_s", 0.0) + (0.0 if live_now else st.get("cur_dur", 0.0))
        if obs <= 0:
            continue
        api = by_pi.get(label, 0.0)
        if obs > api:
            by_pi[label] = obs
            op = st.get("cur_op") or "Unknown"
            by_op[op] = by_op.get(op, 0.0) + (obs - api)

    # In-progress recordings are not in the session API yet.
    for d in devices:
        h = d.get("hostname")
        if not h or status.get(h) != "recording":
            continue
        dur = float(d.get("recording_duration_s") or 0)
        label = labels.get(h, h)
        op = C.clean_str(d.get("operator"))
        by_pi[label] = by_pi.get(label, 0.0) + dur
        by_op[op] = by_op.get(op, 0.0) + dur

    R.jset("leaderboard", {
        "pi": C.ranked(by_pi, hide_unnamed=True),
        "operator": C.ranked(by_op),
        "by_pi": by_pi,
        "by_op": by_op,
        "source": "api",
        "updated": int(time.time() * 1000),
    }, ttl=600)


# ── Backfill ──────────────────────────────────────────────────────────────
def backfill_due() -> bool:
    last = R.jget("backfill:last", 0) or 0
    return (time.time() - float(last)) >= C.BACKFILL_INTERVAL_SEC


def backfill(force: bool = False) -> dict:
    """Full session-history sweep: on first boot, then hourly.

    Populates the per-Pi session cache, the per-day hours used by the charts,
    and the completed-task history.
    """
    if not force and not backfill_due():
        return {"skipped": "not due"}
    if not R.acquire_lock("backfill", ttl=120):
        return {"skipped": "locked"}

    try:
        R.jset("backfill:last", time.time())
        devices = fleet.fleet_status()
        if not devices:
            return {"skipped": "no devices"}

        named = [(d.get("hostname"), C.device_label(d)) for d in devices
                 if d.get("hostname") and not C.is_unnamed_pi(C.device_label(d))]

        deadline = time.time() + SESSION_FETCH_BUDGET * 2
        fetched = {}
        with ThreadPoolExecutor(max_workers=SESSION_FETCH_WORKERS) as ex:
            futs = {ex.submit(fleet.device_sessions, h): (h, l) for h, l in named}
            try:
                for fut in as_completed(futs, timeout=max(1.0, deadline - time.time())):
                    h, l = futs[fut]
                    try:
                        fetched[h] = (l, fut.result() or [])
                    except Exception:
                        pass
            except Exception:
                pass    # partial results are fine; next hour picks up the rest

        today = C.get_date_str()
        tasks_by_host = {}
        sessions_by_host = {}

        for host, (label, groups) in fetched.items():
            existing = R.hgetall_json("sessions:" + host)
            new_sessions, new_tasks = {}, {}
            for rec in groups:
                dur = float(rec.get("duration_s") or 0)
                if dur <= 0:
                    continue
                sid = rec.get("id") or rec.get("session_uuid") or f"{label}|{rec.get('name')}"
                entry = {
                    "date": C.session_date(rec),
                    "duration_s": dur,
                    "operator": C.clean_str(rec.get("operator")),
                    "name": rec.get("name"),
                    "task": C.task_label(rec),
                    "start_unix": C.session_start_unix(rec),
                }
                if existing.get(sid) != entry:
                    new_sessions[sid] = entry
                new_tasks[sid] = {
                    "label": label,
                    "operator": entry["operator"],
                    "task": entry["task"],
                    "duration_s": dur,
                    "start_time": entry["start_unix"],
                }
            if new_sessions:
                sessions_by_host[host] = new_sessions
            if new_tasks:
                tasks_by_host[host] = new_tasks

        for host, m in sessions_by_host.items():
            R.hset_many_json("sessions:" + host, m)
        for host, m in tasks_by_host.items():
            R.hset_many_json("tasks:" + host, m)

        # Per-day totals for the chart, excluding today (handled live) and
        # weekends (excluded by request).
        by_day = {}
        for host, _ in named:
            for sess in R.hgetall_json("sessions:" + host).values():
                if not isinstance(sess, dict):
                    continue
                ds = sess.get("date")
                if not ds or ds == today or C.is_weekend(ds):
                    continue
                by_day[ds] = by_day.get(ds, 0.0) + float(sess.get("duration_s") or 0)

        if by_day:
            existing_days = R.hgetall_json("daily_hours")
            merged = {d: v for d, v in by_day.items()
                      if v > float(existing_days.get(d, 0) or 0)}
            if merged:
                R.hset_many_json("daily_hours", merged)

        _log(f"INFO   Backfill — {len(fetched)} device(s), {len(by_day)} past day(s).")
        return {"devices": len(fetched), "days": len(by_day)}
    finally:
        R.release_lock("backfill")
