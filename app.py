"""
CPAC Checking Data — reconcile local SQLite with cloud DynamoDB.

Runs as a one-shot job (see systemd/cpac-checking-data.timer).

Compare strategy:
  - Key for mixer:       (sideID, mixerID)
  - Key for calibration: (sideID, calibrationID)
  - Both sides present + updateAt differs  -> newer wins, propagate
  - Only one side present                  -> create on the missing side
"""

import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Tuple

CLOUD_BASE = os.environ.get(
    "CLOUD_API_BASE_URL",
    "https://6cq2hsx83h.execute-api.ap-southeast-1.amazonaws.com",
).rstrip("/")

DB_PATH = os.environ.get(
    "LOCAL_DB_PATH",
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "service-cpac-send-iot",
        "local_iot_data.db",
    ),
)

DEFAULT_SIDE_ID = os.environ.get("CPAC_SIDE_ID", "cpac-riverside")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "15"))

DEFAULT_OFFSET = {"slope": 1.0, "offset": 0.0}
DEFAULT_SETPOINT = {"upper": 80.0, "middle": 50.0, "lower": 20.0}
DEFAULT_CALIBRATION = {
    "calibration1": 0.0,
    "calibration2": 0.0,
    "calibration3": 1.0,
    "offset": 0.0,
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _http(method: str, path: str, payload: Optional[dict] = None) -> Any:
    url = f"{CLOUD_BASE}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as res:
        body = res.read().decode("utf-8")
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return body


def cloud_post(path: str, payload: dict) -> Any:
    return _http("POST", path, payload)


# ── Parsing helpers ───────────────────────────────────────────────────────────

def parse_json_field(value: Any, default: dict) -> dict:
    if isinstance(value, dict):
        return value
    if value is None or value == "":
        return default
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else default
    except (TypeError, json.JSONDecodeError):
        return default


def to_int(value: Any) -> int:
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return False


# ── Cloud fetchers ────────────────────────────────────────────────────────────

def fetch_cloud_mixers() -> List[dict]:
    res = cloud_post("/cpac-mixer/get-all", {"sideID": DEFAULT_SIDE_ID})
    return res if isinstance(res, list) else []


def fetch_cloud_calibrations() -> List[dict]:
    res = cloud_post("/cpac-mixer/get-sensor-calibration", {"sideID": DEFAULT_SIDE_ID})
    return res if isinstance(res, list) else []


# ── Local readers ─────────────────────────────────────────────────────────────

def load_local_mixers(conn: sqlite3.Connection) -> List[dict]:
    rows = conn.execute(
        "SELECT sideID, mixerID, active, offset, setpoint, updateAt FROM cpac_mixer"
    ).fetchall()
    return [dict(row) for row in rows]


def load_local_calibrations(conn: sqlite3.Connection) -> List[dict]:
    rows = conn.execute(
        "SELECT sideID, calibrationID, active, calibration, updateAt FROM cpac_sensor_calibration"
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_mixer_for_log(m: dict) -> dict:
    return {
        "sideID": m.get("sideID"),
        "mixerID": m.get("mixerID"),
        "active": to_bool(m.get("active")),
        "offset": parse_json_field(m.get("offset"), DEFAULT_OFFSET),
        "setpoint": parse_json_field(m.get("setpoint"), DEFAULT_SETPOINT),
        "updateAt": to_int(m.get("updateAt")),
    }


def _normalize_calibration_for_log(c: dict) -> dict:
    return {
        "sideID": c.get("sideID"),
        "calibrationID": c.get("calibrationID"),
        "active": to_bool(c.get("active")),
        "calibration": parse_json_field(c.get("calibration"), DEFAULT_CALIBRATION),
        "updateAt": to_int(c.get("updateAt")),
    }


def log_dataset(label: str, items: List[dict], normalizer) -> None:
    print(f"[{label}] count={len(items)}")
    for it in items:
        print(f"  - {json.dumps(normalizer(it), ensure_ascii=False, sort_keys=True)}")


# ── Local writers ─────────────────────────────────────────────────────────────

def upsert_mixer_local(conn: sqlite3.Connection, m: dict) -> None:
    side = m.get("sideID") or DEFAULT_SIDE_ID
    mixer_id = m.get("mixerID")
    if not mixer_id:
        return

    active = 1 if to_bool(m.get("active")) else 0
    offset = json.dumps(parse_json_field(m.get("offset"), DEFAULT_OFFSET))
    setpoint = json.dumps(parse_json_field(m.get("setpoint"), DEFAULT_SETPOINT))
    update_at = to_int(m.get("updateAt")) or int(time.time() * 1000)

    existing = conn.execute(
        "SELECT uid FROM cpac_mixer WHERE sideID = ? AND mixerID = ?",
        (side, mixer_id),
    ).fetchone()

    if existing:
        conn.execute(
            "UPDATE cpac_mixer SET active = ?, offset = ?, setpoint = ?, updateAt = ? "
            "WHERE sideID = ? AND mixerID = ?",
            (active, offset, setpoint, update_at, side, mixer_id),
        )
    else:
        conn.execute(
            "INSERT INTO cpac_mixer (sideID, mixerID, active, offset, setpoint, updateAt) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (side, mixer_id, active, offset, setpoint, update_at),
        )


def upsert_calibration_local(conn: sqlite3.Connection, cal: dict) -> None:
    side = cal.get("sideID") or DEFAULT_SIDE_ID
    cid = cal.get("calibrationID")
    if not cid:
        return

    active = 1 if to_bool(cal.get("active")) else 0
    calibration = json.dumps(parse_json_field(cal.get("calibration"), DEFAULT_CALIBRATION))
    update_at = to_int(cal.get("updateAt")) or int(time.time() * 1000)

    existing = conn.execute(
        "SELECT uid FROM cpac_sensor_calibration WHERE sideID = ? AND calibrationID = ?",
        (side, cid),
    ).fetchone()

    if existing:
        conn.execute(
            "UPDATE cpac_sensor_calibration SET active = ?, calibration = ?, updateAt = ? "
            "WHERE sideID = ? AND calibrationID = ?",
            (active, calibration, update_at, side, cid),
        )
    else:
        conn.execute(
            "INSERT INTO cpac_sensor_calibration (sideID, calibrationID, active, calibration, updateAt) "
            "VALUES (?, ?, ?, ?, ?)",
            (side, cid, active, calibration, update_at),
        )


# ── Cloud writers ─────────────────────────────────────────────────────────────

def push_mixer_to_cloud(m: dict, action: str) -> None:
    """action: 'create' or 'update'."""
    side = m.get("sideID") or DEFAULT_SIDE_ID
    mixer_id = m.get("mixerID")
    if not mixer_id:
        return

    offset = parse_json_field(m.get("offset"), DEFAULT_OFFSET)
    setpoint = parse_json_field(m.get("setpoint"), DEFAULT_SETPOINT)

    try:
        if action == "create":
            cloud_post(
                "/cpac-mixer/create",
                {
                    "mixerID": mixer_id,
                    "offset": offset,
                    "setpoint": setpoint,
                    "source": "local",
                },
            )
        else:
            cloud_post(
                "/cpac-mixer/update",
                {
                    "sideID": side,
                    "mixerID": mixer_id,
                    "offset": offset,
                    "setpoint": setpoint,
                    "source": "local",
                },
            )
    except (urllib.error.URLError, urllib.error.HTTPError) as exc:
        print(f"[mixer {action} cloud] {mixer_id} failed: {exc}", file=sys.stderr)


def push_calibration_to_cloud(cal: dict) -> None:
    """updateSensorCalibration upserts, so same API for create + update."""
    side = cal.get("sideID") or DEFAULT_SIDE_ID
    cid = cal.get("calibrationID")
    if not cid:
        return

    calibration = parse_json_field(cal.get("calibration"), DEFAULT_CALIBRATION)
    active = to_bool(cal.get("active"))

    try:
        cloud_post(
            "/cpac-mixer/update-sensor-calibration",
            {
                "sideID": side,
                "calibrationID": cid,
                "active": active,
                "calibration": calibration,
                "source": "local",
            },
        )
    except (urllib.error.URLError, urllib.error.HTTPError) as exc:
        print(f"[calibration push cloud] {cid} failed: {exc}", file=sys.stderr)


# ── Reconcile ─────────────────────────────────────────────────────────────────

def _index_by(items: Iterable[dict], *keys: str) -> Dict[Tuple[str, ...], dict]:
    out: Dict[Tuple[str, ...], dict] = {}
    for it in items:
        k = tuple(str(it.get(key, "")) for key in keys)
        if all(k):
            out[k] = it
    return out


def reconcile_mixers(
    conn: sqlite3.Connection,
    cloud_list: List[dict],
    local_list: List[dict],
) -> None:
    local_idx = _index_by(local_list, "sideID", "mixerID")
    cloud_idx = _index_by(cloud_list, "sideID", "mixerID")

    for key in set(local_idx.keys()) | set(cloud_idx.keys()):
        local = local_idx.get(key)
        cloud = cloud_idx.get(key)

        if local and cloud:
            l_at = to_int(local.get("updateAt"))
            c_at = to_int(cloud.get("updateAt"))
            if l_at == c_at:
                continue
            if l_at > c_at:
                print(f"[mixer] local newer -> cloud: {key}")
                push_mixer_to_cloud(local, action="update")
            else:
                print(f"[mixer] cloud newer -> local: {key}")
                upsert_mixer_local(conn, cloud)
        elif local:
            print(f"[mixer] local-only -> cloud create: {key}")
            push_mixer_to_cloud(local, action="create")
        elif cloud:
            print(f"[mixer] cloud-only -> local insert: {key}")
            upsert_mixer_local(conn, cloud)

    conn.commit()


def reconcile_calibrations(
    conn: sqlite3.Connection,
    cloud_list: List[dict],
    local_list: List[dict],
) -> None:
    local_idx = _index_by(local_list, "sideID", "calibrationID")
    cloud_idx = _index_by(cloud_list, "sideID", "calibrationID")

    for key in set(local_idx.keys()) | set(cloud_idx.keys()):
        local = local_idx.get(key)
        cloud = cloud_idx.get(key)

        if local and cloud:
            l_at = to_int(local.get("updateAt"))
            c_at = to_int(cloud.get("updateAt"))
            if l_at == c_at:
                continue
            if l_at > c_at:
                print(f"[calibration] local newer -> cloud: {key}")
                push_calibration_to_cloud(local)
            else:
                print(f"[calibration] cloud newer -> local: {key}")
                upsert_calibration_local(conn, cloud)
        elif local:
            print(f"[calibration] local-only -> cloud upsert: {key}")
            push_calibration_to_cloud(local)
        elif cloud:
            print(f"[calibration] cloud-only -> local insert: {key}")
            upsert_calibration_local(conn, cloud)

    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    print(f"[checking-data] db={DB_PATH}")
    print(f"[checking-data] cloud={CLOUD_BASE}")
    print(f"[checking-data] sideID={DEFAULT_SIDE_ID}")

    if not os.path.exists(DB_PATH):
        print(f"[checking-data] DB not found: {DB_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row

    try:
        try:
            cloud_mixers = fetch_cloud_mixers()
            local_mixers = load_local_mixers(conn)
            log_dataset("mixer cloud", cloud_mixers, _normalize_mixer_for_log)
            log_dataset("mixer local", local_mixers, _normalize_mixer_for_log)
            reconcile_mixers(conn, cloud_mixers, local_mixers)
        except Exception as exc:
            print(f"[mixer] reconcile failed: {exc}", file=sys.stderr)

        try:
            cloud_cals = fetch_cloud_calibrations()
            local_cals = load_local_calibrations(conn)
            log_dataset("calibration cloud", cloud_cals, _normalize_calibration_for_log)
            log_dataset("calibration local", local_cals, _normalize_calibration_for_log)
            reconcile_calibrations(conn, cloud_cals, local_cals)
        except Exception as exc:
            print(f"[calibration] reconcile failed: {exc}", file=sys.stderr)
    finally:
        conn.close()

    print("[checking-data] done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
