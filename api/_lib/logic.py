"""
The work that the old `while True:` loop used to do, reshaped into two
idempotent operations that any invocation can safely run:

  poll()     — one iteration of the fleet loop, driven by open browser tabs
  backfill() — full session history sweep, on first boot and then hourly

Both take a Redis lock, so several tabs polling at once cannot double-count.
"""

import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import core as C
from . import fleet
from . import redis_state as R

# Budget for fanning out per-device session fetches. Vercel kills the
# invocation at maxDuration, so this stays well inside it.
SESSION_FETCH_BUDGET = 8.0
SESSION_FETCH_WORKERS = 12

# Every open tab on every device fires its own /api/summary. The Redis lock
# already stops them doing the work concurrently, but back-to-back requests
# from different devices would still each run a full fleet poll. This gate
# makes the poll cadence a property of the fleet rather than of how many
# tabs happen to be open: the first request in a window does the work, the
# rest get the cached state in well under a second.
#
# Deliberately below POLL_MS (20s) so a single lone tab is never gated by
# its own jitter.
POLL_MIN_INTERVAL_SEC = 15.0

# Outside shift hours the fleet is not doing anything worth watching at 15s
# resolution, so the gate widens to hourly and every open tab coasts on the
# cached state. Evaluated on the fleet's clock, not the viewer's: a tab open
# in another timezone must not throttle while the floor in California is
# still mid-shift, nor keep polling all night because it is morning there.
NIGHT_START_HOUR = 19        # 7pm Pacific
NIGHT_END_HOUR = 9           # 9am Pacific
NIGHT_MIN_INTERVAL_SEC = 3600.0


def is_night(now_pacific=None) -> bool:
    h = (now_pacific or C.pacific_now()).hour
    return h >= NIGHT_START_HOUR or h < NIGHT_END_HOUR


def poll_min_interval() -> float:
    return NIGHT_MIN_INTERVAL_SEC if is_night() else POLL_MIN_INTERVAL_SEC

# Per-device session fetches per poll cycle. The whole fleet does not need
# checking at once — each cycle takes the next slice, so a rig is refreshed
# every ceil(online / RIG_SHARD_TARGET) cycles instead of all of them
# hammering the fleet API on the same tick.
RIG_SHARD_TARGET = 12


def _log(*lines):
    try:
        R.log_push([f"[{C.ts()}] {l}" for l in lines])
    except R.RedisUnavailable:
        pass


def _time_shard(items, per_cycle):
    """The slice of `items` due for checking in the current cycle.

    Keyed off wall-clock time rather than a stored cursor, so it advances
    once per POLL_MIN_INTERVAL_SEC no matter which tab or device happened to
    drive the poll. Striding (items[i::n]) rather than chunking keeps each
    slice spread across the fleet instead of clustering by hostname.
    """
    items = sorted(items)
    if len(items) <= per_cycle:
        return items
    shards = -(-len(items) // per_cycle)      # ceil
    idx = int(time.time() // POLL_MIN_INTERVAL_SEC) % shards
    return items[idx::shards]


def _gather(fn, keys, budget):
    """Run fn over keys in parallel, returning whatever finished within budget.

    `with ThreadPoolExecutor(...)` cannot be used here. Its __exit__ calls
    shutdown(wait=True), which blocks until *every* submitted future is done —
    so a `break` at the deadline, or as_completed() raising, still waited for
    all the outstanding fetches. With 34 rigs online, 8 workers and a 15s
    per-request timeout with one retry, that was up to ~150s against a 60s
    maxDuration, which is what produced the 504s.

    Shutting down with wait=False and cancel_futures=True drops the queued
    work and returns immediately. Anything that did not finish is simply
    absent from the result; callers fall back to the last cached value and
    the next poll picks it up.
    """
    out = {}
    keys = list(keys)
    if not keys:
        return out

    deadline = time.time() + budget
    ex = ThreadPoolExecutor(max_workers=SESSION_FETCH_WORKERS)
    try:
        futs = {ex.submit(fn, k): k for k in keys}
        try:
            for fut in as_completed(futs, timeout=max(0.1, deadline - time.time())):
                try:
                    out[futs[fut]] = fut.result()
                except Exception:
                    pass            # one bad device must not sink the poll
                if time.time() >= deadline:
                    break
        except Exception:
            pass                    # as_completed timed out; keep partials
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    return out


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
        last = float(R.jget("poll:last", 0) or 0)
        age = time.time() - last
        interval = poll_min_interval()
        # `force` is the manual refresh button. It skips the cadence gate but
        # still respects the lock, so mashing it cannot stack fleet sweeps.
        if not force and age < interval:
            return {"skipped": "recent", "age_s": round(age, 1),
                    "interval_s": interval, "night": is_night()}
        R.jset("poll:last", time.time())

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
        service_memory = R.jget("service_memory", {}) or {}
        rigs, alerts = C.evaluate_rigs(
            devices,
            locations=locations,
            should_alert=None,
            prev_status=prev_status,
            service_memory=service_memory,
        )

        # Refresh the memory only from rigs the API actually flagged this
        # cycle. Stamping a sticky rig with `now` would keep renewing its own
        # lease and it could never leave servicing.
        now = time.time()
        new_memory = {}
        for r in rigs:
            if r["status"] != "servicing":
                continue
            host = r["hostname"]
            if r.get("service_sticky"):
                prev = service_memory.get(host)
                if isinstance(prev, dict):
                    new_memory[host] = prev        # keep the original ts; it expires
            else:
                new_memory[host] = {"ts": now, "svc": {
                    "issue": r.get("service_issue") or "",
                    "detail": r.get("service_detail") or "",
                    "by": r.get("service_by") or "",
                    "flagged_at": r.get("service_since"),
                }}
        R.jset("service_memory", new_memory, ttl=3600)

        # "Last seen online" for the offline tiles.
        #
        # The fleet API has its own `last_seen`, but it reports 0 for exactly
        # the devices this is needed for — an unreachable Pi has no last
        # contact time to give. So it is authoritative when non-zero (it is
        # the real last contact, not merely when we happened to poll) and we
        # fall back to stamping the poll time ourselves.
        #
        # Consequence worth knowing: a rig that is already offline when this
        # first runs has no history, and stays blank until it comes back once.
        by_host = {d.get("hostname"): d for d in devices if d.get("hostname")}
        last_online = R.jget("last_online", {}) or {}
        for r in rigs:
            host = r["hostname"]
            api_seen = 0.0
            try:
                api_seen = float((by_host.get(host) or {}).get("last_seen") or 0)
            except (TypeError, ValueError):
                api_seen = 0.0
            # Sanity: a plausible epoch, not a 0/uptime counter.
            if api_seen < 1_000_000_000:
                api_seen = 0.0

            stamp = api_seen or (now if r.get("online") else 0.0)
            if stamp:
                # Monotonic: never let a stale API value walk the time back.
                prev = float(last_online.get(host) or 0)
                last_online[host] = max(prev, stamp)

            if not r.get("online"):
                seen = last_online.get(host)
                r["last_seen"] = float(seen) if seen else None
        # Bounded: drop hosts the fleet no longer reports at all.
        live_hosts = {r["hostname"] for r in rigs}
        last_online = {h: t for h, t in last_online.items() if h in live_hosts}
        R.jset("last_online", last_online)

        active = R.jget("active_alerts", {}) or {}
        current = {f"{a['hostname']}|{a['kind']}": a for a in alerts}

        # Transitions fire exactly once per incident.
        #
        # Two things deliberately do NOT reach the terminal:
        #   recording_stopped — a rig stopping (usually by dropping offline)
        #     already prints a CHANGE line carrying the operator and the run
        #     length, which is the useful record. The CRITICAL was a duplicate
        #     of the same event with less information.
        #   resolutions — every incident was printed twice, and a wall of
        #     "cleared" lines pushed the live faults off the top of the log.
        # Both still raise alerts and notifications; this only governs what
        # gets written to the terminal dump.
        for k, a in current.items():
            if k not in active and a.get("kind") != "recording_stopped":
                _log(f"CRITICAL {a['rig']}: {a['message']}")
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

    # Cached sessions are re-filtered against today, so the fallbacks below
    # cannot drag yesterday's work into today's totals after a rollover.
    cached = {h: [g for g in (v or []) if C.session_is_today(g, today)]
              for h, v in (R.hgetall_json("today_sessions") or {}).items()
              if isinstance(v, list)}
    results = {}

    def _hours(groups):
        return sum(float(g.get("duration_s") or 0) for g in (groups or []))

    # Only this cycle's slice is fetched. Everything else keeps its cached
    # sessions via the fallback below, so the numbers stay complete while the
    # per-poll cost stops scaling with fleet size.
    targets = _time_shard([h for h, s in status.items() if s != "offline"],
                          RIG_SHARD_TARGET)
    for h, groups in _gather(fleet.device_sessions, targets,
                             SESSION_FETCH_BUDGET).items():
        todays = [g for g in (groups or []) if C.session_is_today(g, today)]
        # A device that answers with nothing is almost always a failed or
        # truncated fetch, not a rig that un-recorded its morning. Taking the
        # larger of the two keeps the total monotonic through the day, which
        # is what it should be: today's recorded hours only ever grow.
        #
        # Without this the total visibly collapsed — 70.4h to 41.7h between
        # two consecutive polls — because an empty result overwrote the cache
        # and the "not in results" fallback below could no longer save it.
        results[h] = todays if _hours(todays) >= _hours(cached.get(h)) else cached[h]

    # Devices outside this cycle's slice keep their last known sessions rather
    # than dropping to zero and collapsing the operator totals.
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
        devices = fleet.fleet_status()
        if not devices:
            return {"skipped": "no devices"}

        named = [(d.get("hostname"), C.device_label(d)) for d in devices
                 if d.get("hostname") and not C.is_unnamed_pi(C.device_label(d))]

        # The budget means a sweep usually reaches only some of the fleet.
        # In a fixed order the same devices would win every hour and the tail
        # would never sync at all, so the order is shuffled: each sweep covers
        # a different subset and Redis accumulates the union across hours.
        random.shuffle(named)
        labels = dict(named)
        fetched = {h: (labels[h], groups or [])
                   for h, groups in _gather(fleet.device_sessions, labels,
                                            SESSION_FETCH_BUDGET * 2).items()}

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
                    # Stored so past-day rankings can name the rig. Every
                    # hostname is of the form rpi5-xxxx-xxxx, which trips
                    # is_unnamed_pi() — without the label the session gets
                    # filtered out of stats_range entirely.
                    "label": label,
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

        # Stamped only once the sweep has actually succeeded. Stamping up front
        # meant any failure — an unreachable fleet API, missing credentials —
        # still counted as "done" and suppressed every retry for a full hour.
        # A sweep that reached no device at all is a failure too, not an
        # hour's worth of "done".
        if not fetched:
            return {"skipped": "no sessions fetched"}
        R.jset("backfill:last", time.time())

        _log(f"INFO   Backfill — {len(fetched)} device(s), {len(by_day)} past day(s).")
        return {"devices": len(fetched), "days": len(by_day)}
    finally:
        R.release_lock("backfill")
