"""
The work that the old `while True:` loop used to do, reshaped into two
idempotent operations that any invocation can safely run:

  poll()     — one iteration of the fleet loop, driven by open browser tabs
  backfill() — full session history sweep, on first boot and then hourly

Both take a Redis lock, so several tabs polling at once cannot double-count.

Both also leave behind the answer in the shape the dashboard asks for, rather
than the shape the fleet API gave it: poll() assembles the live view, backfill()
assembles the history. Serving a request is then one read of one key, with no
per-rig fan-out and no reassembly — which is what keeps the Redis command count
and the function's active CPU proportional to how often the FLEET changes
instead of how often somebody is looking at it.
"""

import csv
import os
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
# The floor matches the dashboard's default refresh, so a fleet sweep happens
# once per tick and not twice. It used to sit at 15s while every tab asked
# every 30s, which bought nothing: the extra sweep landed between two ticks and
# nobody ever saw it, and sweeping is by far the most expensive thing a request
# can do. A tab that has chosen a faster refresh says so (see below) and gets
# it; POLL_FLOOR_SEC is how fast anyone may drive the fleet.
POLL_MIN_INTERVAL_SEC = 30.0
POLL_FLOOR_SEC = 15.0

# Outside shift hours the fleet is not doing anything worth watching at 15s
# resolution, so the gate widens to hourly and every open tab coasts on the
# cached state. Evaluated on the fleet's clock, not the viewer's: a tab open
# in another timezone must not throttle while the floor in California is
# still mid-shift, nor keep polling all night because it is morning there.
NIGHT_START_HOUR = 19        # 7pm Pacific
NIGHT_END_HOUR = 9           # 9am Pacific
NIGHT_MIN_INTERVAL_SEC = 3600.0

# How much of the terminal dump the view carries. It is sent to every tab on
# every tick, so it is capped tighter than the durable ring buffer behind it.
LOG_VIEW_MAX = 250

# Enough frame-health points to draw the chart on a fresh load; a tab that is
# already open extends the line from each tick itself.
HEALTH_SEED_MAX = 180

# The frame-health series only needs enough resolution to seed the chart on
# load — a tab that is already open appends each poll's reading to its own
# copy. Pushing one point per poll was two Redis commands every 15 seconds
# for a line the eye cannot resolve at that density.
HEALTH_PUSH_SEC = 120.0

# Locations change when somebody types one in, which is a handful of times a
# month, so re-reading the hash every cycle was a command spent on nothing.
# set_location patches the cached copy directly; this is the backstop for when
# that patch loses a race with a concurrent poll.
LOCATIONS_REFRESH_SEC = 300.0

# A rig that drops offline mid-recording used to freeze forever: nothing ever
# banked its in-progress duration unless it came back and cleanly reported
# idle, so a Pi that never reconnects (dead SD card, someone unplugs it at
# the end of a shift) silently lost the task it was in the middle of. This is
# how long to wait for it to come back with the SAME session still running
# before giving up and finalizing what was captured — long enough that a
# rig rebooting or a wifi blip does not split one recording into two, short
# enough that a task is not stuck in limbo for hours.
OFFLINE_FINALIZE_SEC = 900.0


def is_night(now_pacific=None) -> bool:
    h = (now_pacific or C.pacific_now()).hour
    return h >= NIGHT_START_HOUR or h < NIGHT_END_HOUR


def poll_min_interval(requested=None) -> float:
    """How often the fleet may be swept, in seconds.

    `requested` is the refresh rate the asking tab has chosen. Honouring it
    keeps the rate picker on the dashboard meaningful without making everyone
    else pay for a cadence they did not ask for and cannot see.

    The tab that wins a cycle sets how long the gate holds, so with a mix of
    rates open at once the picker means "as often as", not "exactly": a 15s tab
    waits out a 30s tab's window if that one swept first. Erring that way is
    deliberate — the wrong answer costs a fleet sweep, which is the most
    expensive thing a request can do.
    """
    if is_night():
        return NIGHT_MIN_INTERVAL_SEC
    if requested is None:
        return POLL_MIN_INTERVAL_SEC
    try:
        return max(POLL_FLOOR_SEC, min(float(requested), NIGHT_MIN_INTERVAL_SEC))
    except (TypeError, ValueError):
        return POLL_MIN_INTERVAL_SEC


# Per-device session fetches per poll cycle. The whole fleet does not need
# checking at once — each cycle takes the next slice, so a rig is refreshed
# every ceil(online / RIG_SHARD_TARGET) cycles instead of all of them
# hammering the fleet API on the same tick.
RIG_SHARD_TARGET = 12

# Redis key prefixes for the per-rig hashes the blobs are rebuilt from.
_TASKS = "tasks:"
_SESSIONS = "sessions:"
# Per-rig median bytes/second, rebuilt by each backfill. Small enough to read
# on a request that needs to judge a session's duration against its size.
_BYTE_RATES = "byte_rates"


def _stamp(line: str) -> str:
    return f"[{C.ts()}] {line}"


def _drain_legacy_log():
    """The ring buffer the view's log tail replaced, read once on migration.

    Keeping it in step cost an LPUSH and an LTRIM on every cycle that emitted a
    line, to duplicate 250 lines the state blob already carries durably.
    """
    try:
        return R.log_read(LOG_VIEW_MAX) or []
    except R.RedisUnavailable:
        return []


def _view_log(*lines, history_v=None, last_at=None):
    """Same, but also into the view, so a backfill's line, the history version
    it produced and its completion stamp all land in the one write."""
    stamped = [_stamp(l) for l in lines]
    try:
        st = load_state(fresh=True)
        view = st.get("view")
        if isinstance(view, dict):
            view["logs"] = (list(view.get("logs") or []) + stamped)[-LOG_VIEW_MAX:]
            if history_v:
                view["history_v"] = history_v
        p = st.setdefault("poll", {})
        if history_v:
            p["history_v"] = history_v
        if last_at:
            p["backfill_last"] = last_at
        R.state_save(st)
    except R.RedisUnavailable:
        pass


def _stamp_backfill(try_at):
    """Record that a sweep was attempted, so a failing one backs off."""
    try:
        st = load_state(fresh=True)
        st.setdefault("poll", {})["backfill_try"] = try_at
        R.state_save(st)
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
    idx = int(time.time() // POLL_FLOOR_SEC) % shards
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


# ── State ─────────────────────────────────────────────────────────────────
def load_state(fresh: bool = False) -> dict:
    """The state blob, usually from the container's own memo.

    How long that memo is allowed to stand follows from how often the data can
    possibly change: nothing writes the view except a poll, and a poll cannot
    run more than once per interval, so holding a copy for one interval adds at
    most one interval of staleness to a dashboard that refreshes on the same
    cadence. At night, when the fleet is polled hourly, it is capped at a
    minute rather than an hour — the point is to collapse a burst of ticks from
    several devices into one read, not to serve genuinely old data.

    Callers that are about to WRITE pass fresh=True.
    """
    ttl = 0.0 if fresh else min(poll_min_interval(), 60.0)
    st = R.state_load(ttl)
    if "poll" not in st:
        st = _migrate_state(st)
    return st


def _migrate_state(st: dict) -> dict:
    """One-time import of the keys the poll state used to be spread across.

    Eight commands, once per database rather than a dozen per cycle. Worth it:
    starting from nothing would reprint every live fault as new, blank the
    last-seen time on every offline tile, and reset the self-tracked hours for
    the remainder of the day.
    """
    st = dict(st or {})
    st["poll"] = {
        "last": float(R.jget("poll:last", 0) or 0),
        "prev_status": R.jget("prev_status", {}) or {},
        "service_memory": R.jget("service_memory", {}) or {},
        "last_online": R.jget("last_online", {}) or {},
        "active_alerts": R.jget("active_alerts", {}) or {},
        "observed": R.hgetall_json("observed") or {},
        "locations": R.hgetall_json("locations") or {},
        "locations_at": time.time(),
        "debounce": {},
        "health_at": 0.0,
        "backfill_last": float(R.jget("backfill:last", 0) or 0),
        "backfill_try": float(R.jget("backfill:try", 0) or 0),
    }
    st.setdefault("view", {})
    return st


# ── Poll ──────────────────────────────────────────────────────────────────
def agent_alive(state) -> dict | None:
    """The agent's heartbeat, if one is polling and still current.

    When agent.py is running on a machine you control it owns the fleet loop
    entirely, and the serverless function stops calling out to fleet.shiftiq.us
    at all — it only reads back what the agent left in Redis. Three missed
    cycles (or ninety seconds, whichever is longer) and the function quietly
    takes the loop back, so killing the agent degrades to the old behaviour
    instead of freezing the dashboard.
    """
    a = ((state or {}).get("poll") or {}).get("agent")
    if not isinstance(a, dict):
        return None
    age = time.time() - float(a.get("at") or 0)
    return a if age <= max(3 * float(a.get("every") or 30), 90) else None


def byte_rates(memo_ttl: float = 600.0):
    """The per-rig bytes/second baselines the last backfill wrote.

    Read on the poll path, which is why it is memoised: the rates only move
    when a sweep rebuilds them, once an hour, and a warm instance would
    otherwise spend a billable command per tick re-reading a value that had
    not changed.
    """
    hit = R.memo_get(_BYTE_RATES, memo_ttl)
    if hit is not None:
        return hit
    try:
        return R.memo_set(_BYTE_RATES, R.jget(_BYTE_RATES) or {})
    except R.RedisUnavailable:
        return {}


def _bank_observed(stt: dict):
    """Move the in-progress self-tracked segment into its operator's bank.

    Keyed by operator rather than a single running total, so a rig that
    changes hands during the day keeps each operator's share distinguishable
    later — see the proportional split in _rebuild_leaderboard, which used to
    credit 100% of a rig's self-tracked overflow to whichever operator
    happened to be on it last.
    """
    op = stt.get("cur_op") or "Unknown"
    by_op = stt.setdefault("banked_by_op", {})
    by_op[op] = by_op.get(op, 0.0) + stt["cur_dur"]


def poll(force: bool = False, state=None, agent=None, requested=None):
    """One iteration of the fleet loop.

    Returns (result, state). The state is handed back because the caller is
    about to serve the view out of it, and this is either the blob it passed
    in or the one this cycle just wrote — either way, no second read.

    Safe to call as often as tabs fire it: if another invocation holds the
    lock this returns immediately and the caller reads the cached view.
    """
    st = state if state is not None else load_state()
    p = st.setdefault("poll", {})

    interval = poll_min_interval(requested)
    age = time.time() - float(p.get("last") or 0)
    # Two gates, and the cheap one runs first. This reads the state the caller
    # already loaded to serve the view, so a tick that finds the poll recent —
    # the common case the moment more than one device is watching — costs no
    # Redis command at all. It can be up to one memo-TTL stale, which is why
    # the authoritative gate below still runs.
    if not force and age < interval:
        return {"skipped": "recent", "age_s": round(age, 1),
                "interval_s": interval, "night": is_night()}, st

    # Cadence and mutual exclusion in one command, and never released: the key
    # expiring is the next cycle falling due. Capped so that crossing into
    # daytime does not leave an hour-long night key blocking the fleet.
    #
    # Manual refresh has to stand the gate down before taking it, or pressing
    # the button inside the cadence window would answer "locked" and do
    # nothing. It still has to take it afterwards, so two people pressing at
    # once cannot stack two sweeps.
    if force:
        R.release_lock("poll")
    if not R.acquire_lock("poll", ttl=int(min(interval, 300))):
        return {"skipped": "locked"}, st

    try:
        # Whoever wins the gate reads fresh. Reads elsewhere tolerate a stale
        # memo because nothing changes between polls, but a poll writing back
        # a copy it took twenty seconds ago would drop another container's
        # banked time and re-fire alerts it had already reported.
        st = load_state(fresh=True)
        p = st.setdefault("poll", {})
        p["last"] = time.time()

        devices = fleet.fleet_status()
        if not devices:
            # Stamped anyway, so an outage at the fleet API does not turn
            # every open tab into a retry loop against it.
            R.state_save(st)
            return {"skipped": "no devices"}, st

        today = C.get_date_str()
        now = time.time()
        lines = []

        if now - float(p.get("locations_at") or 0) >= LOCATIONS_REFRESH_SEC:
            p["locations"] = R.hgetall_json("locations") or {}
            p["locations_at"] = now

        # Evaluate with NO debounce, so `alerts` is the set of conditions that
        # are genuinely true right now.
        #
        # These are two different questions and conflating them is a bug:
        #   "is the condition true?"  -> drives CRITICAL/RESOLVED transitions
        #   "should we notify again?" -> the 15-minute quiet period
        # Debouncing the evaluation itself made a still-failing rig look like
        # it had recovered, so the log oscillated CRITICAL -> RESOLVED ->
        # CRITICAL every 15 minutes while nothing actually changed.
        service_memory = p.get("service_memory") or {}
        rigs, alerts = C.evaluate_rigs(
            devices,
            locations=p.get("locations") or {},
            should_alert=None,
            prev_status=p.get("prev_status") or {},
            service_memory=service_memory,
        )

        # Refresh the memory only from rigs the API actually flagged this
        # cycle. Stamping a sticky rig with `now` would keep renewing its own
        # lease and it could never leave servicing.
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
        p["service_memory"] = new_memory

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
        last_online = p.get("last_online") or {}
        for r in rigs:
            host = r["hostname"]
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
                last_online[host] = max(float(last_online.get(host) or 0), stamp)

            if not r.get("online"):
                seen = last_online.get(host)
                r["last_seen"] = float(seen) if seen else None
        # Bounded: drop hosts the fleet no longer reports at all.
        live_hosts = {r["hostname"] for r in rigs}
        p["last_online"] = {h: t for h, t in last_online.items() if h in live_hosts}

        active = p.get("active_alerts") or {}
        current = {f"{a['hostname']}|{a['kind']}": a for a in alerts}

        # When the condition first became true, carried across cycles for as
        # long as it stays true. The notification centre shows it, and "since
        # 09:14" is a different piece of information from "reported now" — a
        # fault nobody has looked at for three hours should read that way.
        for k, a in current.items():
            a["since"] = float((active.get(k) or {}).get("since") or now)

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
                lines.append(f"CRITICAL {a['rig']}: {a['message']}")
        p["active_alerts"] = current

        # The quiet period applies only to re-notifying. A newly-appeared
        # condition always notifies; an ongoing one stays silent until the
        # window lapses. The alert itself is still reported either way, so the
        # rig tile keeps showing the warning.
        debounce = p.get("debounce") or {}
        for k, a in current.items():
            if k not in active or now >= float(debounce.get(k) or 0):
                a["notify"] = True
                debounce[k] = now + C.ALERT_DEBOUNCE_SEC
            else:
                a["notify"] = False
        # Expired entries would otherwise accumulate in the blob forever.
        p["debounce"] = {k: v for k, v in debounce.items() if float(v) > now}

        # Frame health snapshot. "avg"/"min" treat every rig equally
        # regardless of how many frames it actually recorded; "overall" is
        # the fleet's real capture rate — total frames captured over total
        # frames expected, so one rig with a bad session cannot look the same
        # as ten rigs each dropping the same fraction of a much bigger take.
        readings = [r["frame_health_pct"] for r in rigs
                    if r.get("frame_health_pct") is not None and r.get("online")]
        cap_total = sum(r.get("frames_captured") or 0 for r in rigs if r.get("online"))
        exp_total = sum(r.get("frames_expected") or 0 for r in rigs if r.get("online"))
        health_point = None
        if readings:
            health_point = {
                "t": int(now * 1000),
                "avg": round(sum(readings) / len(readings), 2),
                "min": round(min(readings), 2),
                "overall": round(cap_total / exp_total * 100, 2) if exp_total else None,
            }
            if now - float(p.get("health_at") or 0) >= HEALTH_PUSH_SEC:
                p["health"] = (list(p.get("health") or []) + [health_point])[-HEALTH_SEED_MAX:]
                p["health_at"] = now

        # Idempotent observed-time tracker. Preserved verbatim in spirit from
        # the original: max() means re-observing the same recording can never
        # add time twice; a segment is only banked when the operator changes,
        # the counter resets, or the rig cleanly stops.
        observed = p.get("observed") or {}
        new_status = {}
        finished = []
        for d in devices:
            host = d.get("hostname") or ""
            if not host:
                continue
            status = C.get_status(d)
            label = C.device_label(d)
            new_status[host] = status

            stt = observed.get(host)
            if not isinstance(stt, dict) or stt.get("date") != today:
                stt = {"date": today, "banked_by_op": {}, "cur_dur": 0.0,
                       "cur_op": "", "cur_task": "", "label": label,
                       "offline_since": 0.0}
            stt.setdefault("cur_task", "")
            stt.setdefault("offline_since", 0.0)
            stt.setdefault("banked_by_op", {})
            stt["label"] = label

            if status == "recording":
                stt["offline_since"] = 0.0     # back, or never left
                op = C.clean_str(d.get("operator"))
                task = C.clean_str(d.get("task"))
                dur = float(d.get("recording_duration_s") or 0)
                if stt["cur_op"] and (op != stt["cur_op"] or dur < stt["cur_dur"] - 30):
                    _bank_observed(stt)
                    stt["cur_dur"] = dur
                    stt["cur_op"] = op
                    stt["cur_task"] = task
                else:
                    stt["cur_op"] = op
                    stt["cur_task"] = task
                    stt["cur_dur"] = max(stt["cur_dur"], dur)
            elif status == "offline":
                # Freeze while there is nothing in progress. While something
                # IS in progress, give it OFFLINE_FINALIZE_SEC to reconnect
                # before assuming it is not coming back and banking what was
                # captured — otherwise a rig that never reconnects loses the
                # task it was in the middle of, forever.
                if stt["cur_dur"] > 0:
                    if not stt["offline_since"]:
                        stt["offline_since"] = now
                    elif now - float(stt["offline_since"]) >= OFFLINE_FINALIZE_SEC:
                        finished.append({
                            "hostname": host,
                            "label": label,
                            "operator": stt.get("cur_op") or "Unknown",
                            "task": stt.get("cur_task") or "Unknown",
                            "duration_s": stt["cur_dur"],
                            "start_time": int(time.time() - stt["cur_dur"]),
                            "offline": True,
                        })
                        _bank_observed(stt)
                        stt["cur_dur"] = 0.0
                        stt["cur_op"] = ""
                        stt["cur_task"] = ""
                        stt["offline_since"] = 0.0
            else:
                stt["offline_since"] = 0.0
                if stt["cur_dur"] > 0:
                    # Clean stop -> bank it and record the completed task.
                    finished.append({
                        "hostname": host,
                        "label": label,
                        "operator": stt.get("cur_op") or "Unknown",
                        "task": stt.get("cur_task") or C.clean_str(d.get("task")),
                        "duration_s": stt["cur_dur"],
                        "start_time": int(time.time() - stt["cur_dur"]),
                    })
                    _bank_observed(stt)
                    stt["cur_dur"] = 0.0
                    stt["cur_op"] = ""
                    stt["cur_task"] = ""
            observed[host] = stt

        p["observed"] = observed
        p["prev_status"] = new_status

        if finished:
            p["history_v"] = _record_finished(finished)
            _append_task_report(finished)
            for f in finished:
                tag = " | reconnect timed out" if f.get("offline") else ""
                lines.append(f"CHANGE {f['label']}  STOPPED | Op: {f['operator']} | "
                             f"Task: {f['task']} | {C.format_time(f['duration_s'])}{tag}")

            # The recording_stopped alert (fired earlier this same cycle, from
            # the same prev-status transition) carries only hostname/kind/
            # message. `finished` has the operator and task that were actually
            # attached to the session that just ended, so stitch them on here
            # rather than re-deriving them from the device's current state,
            # which may already have moved on.
            finished_by_host = {f["hostname"]: f for f in finished}
            alerted_hosts = set()
            for a in alerts:
                if a.get("kind") != "recording_stopped":
                    continue
                alerted_hosts.add(a.get("hostname"))
                f = finished_by_host.get(a.get("hostname"))
                if f:
                    a["operator"] = f["operator"]
                    a["task"] = f["task"]

            # A task finalized because the rig went offline mid-recording (see
            # OFFLINE_FINALIZE_SEC) never passes through the idle/uploading
            # transition core.evaluate_rigs() alerts on, so without this it
            # would be recorded in history but never reach the notification
            # centre at all.
            for host, f in finished_by_host.items():
                if not f.get("offline") or host in alerted_hosts:
                    continue
                alerts.append({"hostname": host, "rig": f["label"], "kind": "recording_stopped",
                               "message": "Pi stopped recording", "since": now,
                               "operator": f["operator"], "task": f["task"]})

        if "today" not in p:
            p["today"] = _migrate_today(today)
        by_pi, by_op = _rebuild_leaderboard(devices, today, observed, p["today"],
                                            byte_rates())
        p["by_pi"], p["by_op"] = by_pi, by_op

        # Rides along in the blob that is being written anyway, so claiming the
        # loop costs the agent nothing.
        if agent:
            p["agent"] = {"at": time.time(), "id": agent[0], "every": agent[1]}

        # Running total of billable Redis commands, for the dev console.
        # Counted here because this is the one place a container is already
        # writing; a container that only ever serves reads never gets to add
        # its own handful, so the total is a floor rather than an exact figure.
        cmds = p.get("cmds") if isinstance(p.get("cmds"), dict) else {}
        p["cmds"] = {
            "n": float(cmds.get("n") or 0) + R.take_command_delta(),
            "since": float(cmds.get("since") or now),
        }

        st["view"] = _build_view(st, rigs, alerts, by_pi, by_op,
                                 health_point, [_stamp(l) for l in lines])
        R.state_save(st)

        return {"rigs": len(rigs), "alerts": len(alerts),
                "finished": len(finished)}, st
    except Exception:
        # The gate key is the cadence, so it is deliberately NOT released on
        # the happy path. It is released here: a sweep that died should be
        # retried on the next tick, not waited out.
        R.release_lock("poll")
        raise


def _build_view(st, rigs, alerts, by_pi, by_op, health_point, new_lines):
    """Everything a dashboard tick needs, assembled once per poll.

    Assembling it here rather than per request is the whole point: the fleet
    changes every 15 seconds at most, while requests arrive once per tab per
    tick. Anything large and slow-moving — task history, past-day totals, the
    frame-health series — is deliberately NOT in here; it lives in the history
    blob and the client re-fetches that only when its version changes.
    """
    prev = st.get("view") or {}

    logs = list(prev.get("logs") or [])
    # Seeded from the durable ring buffer once, minus the lines that are no
    # longer written but are still sitting in it. Flagged rather than inferred
    # from an empty list, so a genuinely empty log is not re-read every cycle.
    if not st.get("poll", {}).get("logs_seeded"):
        st.setdefault("poll", {})["logs_seeded"] = True
        logs = [l for l in (_drain_legacy_log() or [])
                if " RESOLVED " not in str(l)
                and not (" CRITICAL " in str(l) and "stopped recording" in str(l))]
    logs = (logs + list(new_lines))[-LOG_VIEW_MAX:]

    # Every rig's hours count toward the total, named or not — hiding
    # unnamed rigs is a display concern for the rig leaderboard (below), not
    # a reason to drop their recorded time from the headline number. This
    # used to filter them out here, which made the tile disagree with both
    # /stats_range's total (already unfiltered) and the operator leaderboard.
    total_s = sum(by_pi.values())
    online = [r for r in rigs if r.get("online")]
    live = [r["frame_health_pct"] for r in online if r.get("frame_health_pct") is not None]
    prev_stats = prev.get("stats") or {}
    if live:
        avg_fh, min_fh = round(sum(live) / len(live), 2), round(min(live), 2)
    else:
        # No rig is reporting right now: keep showing the last known reading
        # rather than blanking the tile.
        avg_fh, min_fh = prev_stats.get("avg_frame_health_pct"), prev_stats.get("min_frame_health_pct")

    captured = sum(r.get("frames_captured") or 0 for r in rigs)
    expected = sum(r.get("frames_expected") or 0 for r in rigs)
    dropped = sum(r.get("frames_dropped") or 0 for r in rigs)
    # The fleet's real capture rate — total frames actually captured over
    # total frames expected — as opposed to avg_fh, which weighs a rig that
    # recorded ten minutes the same as one that recorded ten hours.
    online_captured = sum(r.get("frames_captured") or 0 for r in online)
    online_expected = sum(r.get("frames_expected") or 0 for r in online)
    if online_expected:
        overall_fh = round(online_captured / online_expected * 100, 2)
    else:
        overall_fh = prev_stats.get("overall_frame_health_pct")
    now_ms = int(time.time() * 1000)

    return {
        # Bumped once per completed cycle. The client skips re-rendering when
        # it has not moved, which is most ticks once the fleet is quiet.
        "rev": now_ms,
        "devices": {"rigs": rigs, "alerts": alerts, "active": True,
                    # Milliseconds since the fleet was last actually polled, so
                    # the UI can say how stale it is rather than implying live.
                    "updated": now_ms, "night": is_night()},
        "rankings": {"pi": C.ranked(by_pi, hide_unnamed=True),
                     "operator": C.ranked(by_op),
                     "active": True, "source": "api"},
        "stats": {
            "total_hours_today": round(total_s / 3600, 3),
            "avg_frame_health_pct": avg_fh,
            "min_frame_health_pct": min_fh,
            "overall_frame_health_pct": overall_fh,
            "frames_captured_total": captured or None,
            "frames_expected_total": expected or None,
            "frames_dropped_total": dropped or None,
            "active_recording_rigs": sum(1 for r in rigs if r.get("status") == "recording"),
            "total_rigs": len(rigs),
            "last_updated_ms": now_ms,
            # The chart's live point. The seed series comes with the history;
            # an open tab appends this to its own copy each tick, so the line
            # stays at poll resolution without storing it at poll resolution.
            "health_point": health_point,
        },
        "logs": logs,
        # Mirrored in the poll state by whoever last rebuilt the history, so
        # assembling a view never has to open the history key to find out.
        "history_v": (st.get("poll") or {}).get("history_v") or 0,
    }


def _migrate_today(today):
    """Fold the old today_sessions hash into the compact aggregate.

    One HGETALL, once. Skipping it would leave the totals rebuilding from a
    twelve-rig slice per cycle, so today's hours would visibly drop and climb
    back over the following minute — which is exactly the collapse the max()
    merge above exists to prevent.
    """
    out = {}
    rates = byte_rates()
    try:
        for h, groups in (R.hgetall_json("today_sessions") or {}).items():
            if isinstance(groups, list):
                out[h] = _aggregate_sessions(groups, today, h, rates)
    except R.RedisUnavailable:
        pass
    return out


def _aggregate_sessions(groups, today, host=None, rates=None):
    """Today's sessions for one rig, reduced to the two numbers that matter.

    Only the totals are ever read back, so only the totals are kept: storing
    the session lists themselves meant a hash big enough to need its own read
    and write every cycle, and it held several hundred records to answer a
    question about fifty sums.

    Durations are byte-corrected on the way in, the same as the past-day
    totals — otherwise a rig that hung this morning ranks on its wall clock
    until the day rolls over and a backfill sweep finally sees it.
    """
    seen = set()
    total = 0.0
    by_op = {}
    for rec in groups or []:
        if not C.session_is_today(rec, today):
            continue
        sid = rec.get("id") or rec.get("session_uuid") or rec.get("name")
        if sid in seen:
            continue
        seen.add(sid)
        dur, _api_dur = C.session_durations(host, rec, rates)
        if dur <= 0:
            continue
        total += dur
        op = C.clean_str(rec.get("operator"))
        by_op[op] = by_op.get(op, 0.0) + dur
    return {"d": today, "s": round(total, 2), "op": by_op}


def _rebuild_leaderboard(devices, today, observed, cache, rates=None):
    """Today's per-Pi / per-operator totals from the API session lists, topped
    up by our own observed time where the API has not caught up yet.

    `cache` is the per-rig aggregate carried in the state blob; it is updated
    in place and the caller writes it back with everything else.

    NOTE: the self-tracked top-up below cannot be byte-corrected. It exists for
    sessions the API has not reported yet, which is exactly when there are no
    final bytes to judge a duration against — a recording still in progress has
    written only part of what it will. Those land on the API's corrected figure
    once the session finishes and is fetched.
    """
    by_pi, by_op = {}, {}
    status = {d.get("hostname"): C.get_status(d) for d in devices if d.get("hostname")}
    labels = {d.get("hostname"): C.device_label(d) for d in devices if d.get("hostname")}

    # Re-filtered against today, so the fallbacks below cannot drag yesterday's
    # work into today's totals after a rollover.
    for h in [h for h, v in cache.items() if (v or {}).get("d") != today]:
        cache.pop(h)

    # Only this cycle's slice is fetched. Everything else keeps its cached
    # aggregate, so the numbers stay complete while the per-poll cost stops
    # scaling with fleet size.
    targets = _time_shard([h for h, s in status.items() if s != "offline"],
                          RIG_SHARD_TARGET)
    for h, groups in _gather(fleet.device_sessions, targets,
                             SESSION_FETCH_BUDGET).items():
        fresh = _aggregate_sessions(groups, today, h, rates)
        # A device that answers with nothing is almost always a failed or
        # truncated fetch, not a rig that un-recorded its morning. Taking the
        # larger of the two keeps the total monotonic through the day, which
        # is what it should be: today's recorded hours only ever grow.
        #
        # Without this the total visibly collapsed — 70.4h to 41.7h between
        # two consecutive polls — because an empty result overwrote the cache.
        if fresh["s"] >= float((cache.get(h) or {}).get("s") or 0):
            cache[h] = fresh

    for h, agg in cache.items():
        label = labels.get(h, h)
        by_pi[label] = by_pi.get(label, 0.0) + float(agg.get("s") or 0)
        for op, dur in (agg.get("op") or {}).items():
            by_op[op] = by_op.get(op, 0.0) + float(dur)

    # Fold in self-tracked time using max(), never addition, so a host can
    # never be counted from both its API total and its observed total.
    for host, stt in observed.items():
        if not isinstance(stt, dict) or stt.get("date") != today:
            continue
        label = labels.get(host, stt.get("label", host))
        live_now = status.get(host) == "recording"
        by_op_secs = dict(stt.get("banked_by_op") or {})
        if not live_now and stt.get("cur_dur"):
            cur_op = stt.get("cur_op") or "Unknown"
            by_op_secs[cur_op] = by_op_secs.get(cur_op, 0.0) + float(stt["cur_dur"])
        obs = sum(by_op_secs.values())
        if obs <= 0:
            continue
        api = by_pi.get(label, 0.0)
        if obs > api:
            by_pi[label] = obs
            # Split the overflow across the operators who actually banked
            # it, proportional to each one's share — crediting all of it to
            # whichever operator is currently on the rig (the old behaviour)
            # moved hours to the wrong operator on any rig that changed
            # hands during the day, which is why individual operator totals
            # would not add up to this same total.
            extra = obs - api
            for op, secs in by_op_secs.items():
                by_op[op] = by_op.get(op, 0.0) + extra * (secs / obs)

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

    return by_pi, by_op


# ── History ───────────────────────────────────────────────────────────────
# Task history, per-day totals and the frame-health seed. Rebuilt by the
# hourly backfill and patched in place when a recording finishes, so serving
# it is one GET instead of a KEYS plus one HGETALL per rig — which was, on its
# own, most of the Redis traffic and most of the CPU per request.
def _history_touch(history: dict, tasks_by_host: dict):
    tasks = history.setdefault("tasks", {})
    for host, entries in tasks_by_host.items():
        if not entries:
            continue
        # This sweep just re-read these sessions from the API, so where a
        # stored API row describes the same recording, the fresh verdict
        # replaces it outright instead of competing with it in dedupe_tasks —
        # which keeps whichever copy is LONGER and would otherwise preserve a
        # stale duration forever. Live rows are left for dedupe_tasks to
        # resolve as before; it already prefers an API record over them.
        stored = [e for e in (tasks.get(host) or [])
                  if e.get("src") == "live"
                  or not any(C.same_session(e, f) for f in entries)]
        tasks[host] = C.dedupe_tasks(stored + list(entries))
    history["v"] = int(time.time())


def _record_finished(finished):
    """A recording just stopped: patch it into the history blob.

    It used to be written to a per-rig hash as well, which cost one command per
    rig that stopped in the same cycle — ten of them at the end of a shift. The
    blob is durable Redis state in its own right, so that was a second copy of
    the same records earning nothing.
    """
    entries = {}
    for f in finished:
        entries.setdefault(f["hostname"], []).append({
            "label": f["label"], "operator": f["operator"], "task": f["task"],
            "duration_s": f["duration_s"], "start_time": f["start_time"],
            "src": "live",
        })

    history = R.history_load()
    _history_touch(history, entries)
    R.history_save(history)
    return history["v"]


# Same column order as categorize_tasks.py's FIELDS — appended rows have to
# line up under that header, or opening the file in Excel puts a session's
# operator under the "category" column. That script owns the bool is_<code>
# columns (they depend on --rules and are not known here), so a row from
# this module is simply shorter; csv readers treat the missing trailing
# cells as blank, and categorize_tasks.py re-derives them from task_name on
# its next run (see LIVE_SESSION_PREFIX handling there).
_TASK_REPORT_FIELDS = [
    "row_type", "date", "duration_hhmmss", "pi_name", "operator", "task_name",
    "category", "link", "start_time", "end_time", "duration_hours",
    "duration_seconds", "pi_id", "is_categorized", "session_id",
    "total_bytes", "size_label", "upload_status",
]

# categorize_tasks.py pulls the authoritative record for a session straight
# from the fleet API and will drop this placeholder once it sees a real
# session_id land near the same rig/time (see its LIVE_SESSION_PREFIX merge)
# — but a recording that never gets a clean stop (see OFFLINE_FINALIZE_SEC)
# never gets a real session_id at all, so this stays the only record of it.
LIVE_SESSION_PREFIX = "live|"

_TASK_REPORT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "task_report.csv")


def _append_task_report(finished):
    """Append-only, filesystem-local record of every completed task.

    This is the durability backstop Redis is not: a row written here is
    never edited or rewritten by this module, so it survives a lost or
    flushed Redis blob and does not depend on the network being up. Shares
    task_report.csv with categorize_tasks.py (see that script's docstring)
    rather than a separate file, so both feed the same report instead of one
    silently missing what the other captured.

    Silently does nothing where the filesystem is read-only (Vercel
    serverless) — it only actually persists when this runs inside agent.py on
    hardware whoever runs the poller controls.
    """
    try:
        is_new = not os.path.exists(_TASK_REPORT_PATH)
        # utf-8-sig only on creation: the BOM belongs at the very start of
        # the file, and this may be appending to one categorize_tasks.py
        # already wrote.
        with open(_TASK_REPORT_PATH, "a", newline="",
                  encoding="utf-8-sig" if is_new else "utf-8") as fh:
            w = csv.writer(fh)
            if is_new:
                w.writerow(_TASK_REPORT_FIELDS)
            for f in finished:
                dur = float(f["duration_s"])
                start = C.pacific_from_unix(f["start_time"])
                end = C.pacific_from_unix(f["start_time"] + dur)
                w.writerow([
                    "SESSION", start.strftime("%Y-%m-%d"), C.format_time(dur),
                    f["label"], f["operator"], f["task"],
                    # Not this module's call to make — categorize_tasks.py
                    # applies its own rules to task_name on the next run.
                    "none", "",
                    start.strftime("%H:%M:%S"), end.strftime("%H:%M:%S"),
                    round(dur / 3600, 4), round(dur, 2), f["hostname"], "no",
                    f"{LIVE_SESSION_PREFIX}{f['hostname']}|{int(f['start_time'])}",
                    "", "", "offline-reconnect-timeout" if f.get("offline") else "live",
                ])
    except OSError:
        pass


def _history_bootstrap(history: dict) -> bool:
    """Populate the blob from the per-rig hashes it replaced.

    Runs once, on the first backfill after this shipped, so no rig's history
    reads as empty until it happens to record something new. Nothing writes
    those hashes any more; if the blob is ever lost this restores the snapshot
    they hold, and the sweep refills the rest from the fleet API.
    """
    if history.get("tasks"):
        return False
    tasks = {}
    for key in (R.cmd("KEYS", R.P + _TASKS + "*") or []):
        host = str(key).split(_TASKS, 1)[-1]
        entries = []
        for sid, v in R.hgetall_json(_TASKS + host).items():
            if not isinstance(v, dict):
                continue
            e = dict(v)
            e.setdefault("src", "live" if str(sid).startswith("live|") else "api")
            entries.append(e)
        if entries:
            tasks[host] = C.dedupe_tasks(entries)
    history["tasks"] = tasks
    return True


def _reconcile_unknown(entries, fresh):
    """Patch a self-tracked "Unknown" placeholder once the fleet API's own
    record of that session shows up in a backfill sweep.

    A rig that stops offline (see OFFLINE_FINALIZE_SEC) or mid-task without a
    clean operator/task on the device is banked with "Unknown" fields — the
    only information available at the moment it happened, and the reason it
    shows up as Unknown on the dashboard when the official site, reading only
    the API's finished session, does not. `entries` (this host's existing
    history rows) has no session id to look up — a live placeholder never had
    one — so the match is by time overlap against `fresh` (this sweep's
    freshly parsed API sessions for the same host), same as dedupe_tasks
    uses to tell two records apart. Once matched, the API's operator/task/
    duration are authoritative and are copied on in place, which keeps this
    row's identity stable instead of leaving it to be silently dropped as a
    duplicate the next time dedupe_tasks runs.
    """
    if not entries or not fresh:
        return
    for e in entries:
        if e.get("src") != "live":
            continue
        if e.get("operator") != "Unknown" and e.get("task") != "Unknown":
            continue
        for f in fresh:
            if not C.same_session(e, f):
                continue
            e["operator"] = f["operator"]
            e["task"] = f["task"]
            e["duration_s"] = f["duration_s"]
            e["start_time"] = f["start_time"]
            e["src"] = "api"
            # f's duration may be the byte estimate rather than the API's own
            # span; carry the rejected figure across too, or this row would
            # show a corrected duration with nothing to explain it.
            if f.get("duration_api_s") is None:
                e.pop("duration_api_s", None)
            else:
                e["duration_api_s"] = f["duration_api_s"]
            break


# ── Backfill ──────────────────────────────────────────────────────────────
BACKFILL_RETRY_SEC = 300.0


def backfill_due(state=None) -> bool:
    """Is an hourly sweep due? Answered from the state blob, at no cost.

    This runs on every request that is allowed to poll, so reading a timestamp
    key here was one billable command per tick on the otherwise-free path —
    the single largest remaining cost once the poll itself was gated.

    An empty history is due now whatever the hourly clock says: the stamp is
    shared with whatever ran before, so a fresh deploy or a lost blob would
    otherwise show no task history and no day chart until the top of the next
    hour. That case is gated on the last ATTEMPT rather than the last success,
    so a sweep that keeps failing retries every few minutes instead of on
    every request.
    """
    p = ((state if state is not None else load_state()).get("poll") or {})
    now = time.time()
    if not p.get("history_v"):
        return (now - float(p.get("backfill_try") or 0)) >= BACKFILL_RETRY_SEC
    return (now - float(p.get("backfill_last") or 0)) >= C.BACKFILL_INTERVAL_SEC


def _past_day_totals(named, all_sessions, labels, today, rates):
    """Per-day hours and the per-day rig/operator split, from stored sessions.

    Excludes today (handled live) and weekends (excluded by request). Counts
    the byte-implied duration wherever the session's size contradicts the API's
    wall clock.

    Returns (by_day, breakdown).
    """
    by_day, breakdown = {}, {}
    for host, _ in named:
        stored = all_sessions.get(host)
        if stored is None:
            stored = R.hgetall_json(_SESSIONS + host)
        for sess in stored.values():
            if not isinstance(sess, dict):
                continue
            ds = sess.get("date")
            if not ds or ds == today or C.is_weekend(ds):
                continue
            dur, _api_dur = C.session_durations(host, sess, rates)
            if dur <= 0:
                continue
            by_day[ds] = by_day.get(ds, 0.0) + dur
            day = breakdown.setdefault(ds, {"pi": {}, "op": {}})
            label = sess.get("label") or labels.get(host) or host
            day["pi"][label] = day["pi"].get(label, 0.0) + dur
            op = sess.get("operator") or "Unknown"
            day["op"][op] = day["op"].get(op, 0.0) + dur
    return by_day, breakdown


def backfill(force: bool = False) -> dict:
    """Full session-history sweep: on first boot, then hourly.

    Populates the per-Pi session cache, the per-day hours used by the charts,
    and the completed-task history — all of which end up in the history blob
    the dashboard actually reads.
    """
    if not force and not backfill_due():
        return {"skipped": "not due"}
    if not R.acquire_lock("backfill", ttl=120):
        return {"skipped": "locked"}

    try:
        _stamp_backfill(try_at=time.time())
        devices = fleet.fleet_status()
        if not devices:
            return {"skipped": "no devices"}

        # Unnamed rigs are still real rigs with real hours — hiding them from
        # the leaderboard is a display concern (C.ranked(hide_unnamed=True)),
        # not a reason to skip fetching their sessions and leave their time
        # out of the day totals.
        named = [(d.get("hostname"), C.device_label(d)) for d in devices
                 if d.get("hostname")]

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
        session_count = 0

        # Loaded now rather than after the sweep, so the per-host loop below
        # can reconcile "Unknown" placeholders against this sweep's freshly
        # fetched sessions before they are written back.
        history = R.history_load(0)
        _history_bootstrap(history)
        history_tasks = history["tasks"]

        # Every rig's stored sessions, so the per-day pass below does not
        # re-read the hashes this loop has just read and written.
        all_sessions = {}

        # Sessions first, task records second. The task list needs the byte
        # rates to know which durations to trust, and the rates are only
        # correct once every session this sweep fetched has been merged in.
        fetched_entries = {}
        for host, (label, groups) in fetched.items():
            existing = R.hgetall_json(_SESSIONS + host)
            all_sessions[host] = existing
            new_sessions, entries = {}, []
            for rec in groups:
                dur = float(rec.get("duration_s") or 0)
                if dur <= 0:
                    continue
                session_count += 1
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
                    # What the recording actually weighs, and so how long it
                    # really ran — see C.session_durations(). Sessions stored
                    # before this field existed differ from the entry built
                    # here and are rewritten with it on the next sweep. A
                    # session still uploading reports no size yet; keep the
                    # stored one rather than writing that gap back.
                    "size_bytes": (C.extract_size_bytes(rec)
                                   or (existing.get(sid) or {}).get("size_bytes")),
                }
                if existing.get(sid) != entry:
                    new_sessions[sid] = entry
                entries.append(entry)
            if new_sessions:
                sessions_by_host[host] = new_sessions
                existing.update(new_sessions)
            if entries:
                fetched_entries[host] = entries

        for host, m in sessions_by_host.items():
            R.hset_many_json(_SESSIONS + host, m)

        # The rigs this sweep did not reach still have stored sessions, and the
        # per-day pass below needs them regardless. Reading them here instead
        # costs the same one HGETALL per rig in a single round trip, and lets
        # the baselines below be built from the whole fleet rather than just
        # the shard this sweep happened to fetch.
        missing = [h for h, _ in named if h not in all_sessions]
        for h, stored in zip(missing, R.hgetall_many_json(_SESSIONS + h for h in missing)):
            all_sessions[h] = stored

        # Rebuilt every sweep, then kept in Redis so a request can correct a
        # session's duration without re-reading the fleet's history itself.
        rates = C.byte_rate_baselines(all_sessions)
        R.jset(_BYTE_RATES, rates)

        for host, entries in fetched_entries.items():
            new_tasks = []
            for entry in entries:
                # dur is what the history counts; api_dur is only carried so
                # the dashboard can show, in red, the number it replaced.
                dur, api_dur = C.session_durations(host, entry, rates)
                task = {
                    "label": entry["label"],
                    "operator": entry["operator"],
                    "task": entry["task"],
                    "duration_s": dur,
                    "start_time": entry["start_unix"],
                    "src": "api",
                }
                if api_dur is not None:
                    task["duration_api_s"] = api_dur
                new_tasks.append(task)
            tasks_by_host[host] = new_tasks
            _reconcile_unknown(history_tasks.get(host), new_tasks)
        # The task records go straight into the history blob below. The
        # per-rig session hashes above stay: they accumulate across sweeps that
        # each only reach part of the fleet, and the past-day totals are
        # rebuilt from all of them.

        # Per-day totals for the chart, excluding today (handled live) and
        # weekends (excluded by request). The same pass builds the per-day
        # rig/operator split, so /stats_range no longer has to re-read every
        # session hash on every request.
        by_day, breakdown = _past_day_totals(
            named, all_sessions, labels, today, rates)

        if by_day:
            existing_days = R.hgetall_json("daily_hours")
            # Written whenever the recomputed figure differs, in either
            # direction. This hash used to climb only, so an unreachable rig
            # could not erase hours it had already reported — but history["days"]
            # below, which is what the dashboard actually charts, is rebuilt
            # wholesale from this same by_day every sweep. So the guard only
            # ever protected a copy nobody reads, at the cost of a second,
            # divergent truth that a corrected duration could never fix.
            changed = {d: v for d, v in by_day.items()
                       if abs(v - float(existing_days.get(d, 0) or 0)) > 1.0}
            if changed:
                R.hset_many_json("daily_hours", changed)

            # /day_tasks answers past days out of a per-date cache built once
            # and kept forever. Any day whose total moved has to lose that
            # cache, or the drill-down keeps listing the durations it was built
            # with — including a session this sweep has since stopped
            # correcting. Keyed off the days that moved, so settled days are
            # not re-fetched every sweep.
            for d in changed:
                R.cmd("DEL", R.P + "daytasks:" + d)

        # Stamped only once the sweep has actually succeeded. Stamping up front
        # meant any failure — an unreachable fleet API, missing credentials —
        # still counted as "done" and suppressed every retry for a full hour.
        # A sweep that reached no device at all is a failure too, not an
        # hour's worth of "done".
        if not fetched:
            return {"skipped": "no sessions fetched"}

        _history_touch(history, tasks_by_host)
        if by_day:
            history["days"] = [{"date": d, "hours": round(s / 3600, 3)}
                               for d, s in sorted(by_day.items())]
            history["breakdown"] = breakdown
        history["health"] = list((R.state_load().get("poll") or {}).get("health")
                                 or R.health_read(360))
        R.history_save(history)

        _view_log(f"INFO   Backfill — {len(fetched)} device(s), {len(by_day)} past day(s).",
                  history_v=history.get("v"), last_at=time.time())
        return {"devices": len(fetched), "rigs": len(fetched),
                "sessions": session_count, "days": len(by_day)}
    finally:
        R.release_lock("backfill")
