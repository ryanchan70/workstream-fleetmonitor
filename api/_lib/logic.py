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

# Redis key prefixes for the per-rig hashes the blobs are rebuilt from.
_TASKS = "tasks:"
_SESSIONS = "sessions:"


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


def poll(force: bool = False, state=None, agent=None):
    """One iteration of the fleet loop.

    Returns (result, state). The state is handed back because the caller is
    about to serve the view out of it, and this is either the blob it passed
    in or the one this cycle just wrote — either way, no second read.

    Safe to call as often as tabs fire it: if another invocation holds the
    lock this returns immediately and the caller reads the cached view.
    """
    st = state if state is not None else load_state()
    p = st.setdefault("poll", {})

    interval = poll_min_interval()
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

        # Frame health snapshot.
        readings = [r["frame_health_pct"] for r in rigs
                    if r.get("frame_health_pct") is not None and r.get("online")]
        health_point = None
        if readings:
            health_point = {
                "t": int(now * 1000),
                "avg": round(sum(readings) / len(readings), 2),
                "min": round(min(readings), 2),
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
                stt = {"date": today, "banked_s": 0.0, "cur_dur": 0.0,
                       "cur_op": "", "label": label}
            stt["label"] = label

            if status == "recording":
                op = C.clean_str(d.get("operator"))
                dur = float(d.get("recording_duration_s") or 0)
                if stt["cur_op"] and (op != stt["cur_op"] or dur < stt["cur_dur"] - 30):
                    stt["banked_s"] += stt["cur_dur"]
                    stt["cur_dur"] = dur
                    stt["cur_op"] = op
                else:
                    stt["cur_op"] = op
                    stt["cur_dur"] = max(stt["cur_dur"], dur)
            elif status == "offline":
                pass    # freeze; reconnecting mid-recording just raises cur_dur
            else:
                if stt["cur_dur"] > 0:
                    # Clean stop -> bank it and record the completed task.
                    finished.append({
                        "hostname": host,
                        "label": label,
                        "operator": stt.get("cur_op") or "Unknown",
                        "task": C.clean_str(d.get("task")),
                        "duration_s": stt["cur_dur"],
                        "start_time": int(time.time() - stt["cur_dur"]),
                    })
                    stt["banked_s"] += stt["cur_dur"]
                    stt["cur_dur"] = 0.0
                    stt["cur_op"] = ""
            observed[host] = stt

        p["observed"] = observed
        p["prev_status"] = new_status

        if finished:
            p["history_v"] = _record_finished(finished)
            for f in finished:
                lines.append(f"CHANGE {f['label']}  STOPPED | Op: {f['operator']} | "
                             f"{C.format_time(f['duration_s'])}")

        if "today" not in p:
            p["today"] = _migrate_today(today)
        by_pi, by_op = _rebuild_leaderboard(devices, today, observed, p["today"])
        p["by_pi"], p["by_op"] = by_pi, by_op

        # Rides along in the blob that is being written anyway, so claiming the
        # loop costs the agent nothing.
        if agent:
            p["agent"] = {"at": time.time(), "id": agent[0], "every": agent[1]}

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

    total_s = sum(v for k, v in by_pi.items() if not C.is_unnamed_pi(k))
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
    dropped = sum(r.get("frames_dropped") or 0 for r in rigs)
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
            "frames_captured_total": captured or None,
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
    try:
        for h, groups in (R.hgetall_json("today_sessions") or {}).items():
            if isinstance(groups, list):
                out[h] = _aggregate_sessions(groups, today)
    except R.RedisUnavailable:
        pass
    return out


def _aggregate_sessions(groups, today):
    """Today's sessions for one rig, reduced to the two numbers that matter.

    Only the totals are ever read back, so only the totals are kept: storing
    the session lists themselves meant a hash big enough to need its own read
    and write every cycle, and it held several hundred records to answer a
    question about fifty sums.
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
        dur = float(rec.get("duration_s") or 0)
        if dur <= 0:
            continue
        total += dur
        op = C.clean_str(rec.get("operator"))
        by_op[op] = by_op.get(op, 0.0) + dur
    return {"d": today, "s": round(total, 2), "op": by_op}


def _rebuild_leaderboard(devices, today, observed, cache):
    """Today's per-Pi / per-operator totals from the API session lists, topped
    up by our own observed time where the API has not caught up yet.

    `cache` is the per-rig aggregate carried in the state blob; it is updated
    in place and the caller writes it back with everything else.
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
        fresh = _aggregate_sessions(groups, today)
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
        obs = stt.get("banked_s", 0.0) + (0.0 if live_now else stt.get("cur_dur", 0.0))
        if obs <= 0:
            continue
        api = by_pi.get(label, 0.0)
        if obs > api:
            by_pi[label] = obs
            op = stt.get("cur_op") or "Unknown"
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
        tasks[host] = C.dedupe_tasks(list(tasks.get(host) or []) + list(entries))
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
        session_count = 0

        # Every rig's stored sessions, so the per-day pass below does not
        # re-read the hashes this loop has just read and written.
        all_sessions = {}

        for host, (label, groups) in fetched.items():
            existing = R.hgetall_json(_SESSIONS + host)
            all_sessions[host] = existing
            new_sessions, new_tasks = {}, []
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
                }
                if existing.get(sid) != entry:
                    new_sessions[sid] = entry
                new_tasks.append({
                    "label": label,
                    "operator": entry["operator"],
                    "task": entry["task"],
                    "duration_s": dur,
                    "start_time": entry["start_unix"],
                    "src": "api",
                })
            if new_sessions:
                sessions_by_host[host] = new_sessions
                existing.update(new_sessions)
            if new_tasks:
                tasks_by_host[host] = new_tasks

        for host, m in sessions_by_host.items():
            R.hset_many_json(_SESSIONS + host, m)
        # The task records go straight into the history blob below. The
        # per-rig session hashes above stay: they accumulate across sweeps that
        # each only reach part of the fleet, and the past-day totals are
        # rebuilt from all of them.

        # Per-day totals for the chart, excluding today (handled live) and
        # weekends (excluded by request). The same pass builds the per-day
        # rig/operator split, so /stats_range no longer has to re-read every
        # session hash on every request.
        by_day = {}
        breakdown = {}
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
                dur = float(sess.get("duration_s") or 0)
                if dur <= 0:
                    continue
                by_day[ds] = by_day.get(ds, 0.0) + dur
                day = breakdown.setdefault(ds, {"pi": {}, "op": {}})
                label = sess.get("label") or labels.get(host) or host
                if not C.is_unnamed_pi(label):
                    day["pi"][label] = day["pi"].get(label, 0.0) + dur
                    op = sess.get("operator") or "Unknown"
                    day["op"][op] = day["op"].get(op, 0.0) + dur

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

        history = R.history_load(0)
        _history_bootstrap(history)
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
