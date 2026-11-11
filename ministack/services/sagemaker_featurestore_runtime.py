# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Amazon SageMaker Feature Store Runtime emulator: the online store.

REST-JSON protocol, signed as ``sagemaker``.

Implemented:
  PutRecord, GetRecord, BatchGetRecord, DeleteRecord.

Each record identifier keeps the latest record by event time: a write with an
older event time than the stored one is accepted and ignored, and a write with
the same or a newer event time replaces the whole record. A soft delete leaves
a marker at its event time, so only a newer write brings the record back; a
hard delete removes the record and its history. Records past their TTL are not
returned. Writes to the offline store are accepted and dropped.
"""

import copy
import json
import logging
import math
import re
from datetime import datetime, timezone
from urllib.parse import unquote

from ministack.core.responses import AccountRegionScopedDict, get_account_id, get_region
from ministack.services import sagemaker

logger = logging.getLogger("sagemaker-featurestore-runtime")

_CT = "application/json"

_records = AccountRegionScopedDict()  # feature group name -> {record id: entry}

_INT64 = (-(2 ** 63), 2 ** 63 - 1)
_INTEGRAL_RE = re.compile(r"^[+-]?\d+$")
_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z$")
_TTL_SECONDS = {"Seconds": 1, "Minutes": 60, "Hours": 3600, "Days": 86400, "Weeks": 604800}


def reset():
    _records.clear()


def get_state():
    return {"records": copy.deepcopy(_records)}


def load_persisted_state(data):
    if not data:
        return
    _records.clear()
    _records.update(data.get("records", {}))


def drop_records(group_name):
    _records.pop(group_name, None)


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------


def _ok(body=None):
    if body is None:
        return 200, {"Content-Type": _CT}, b""
    return 200, {"Content-Type": _CT}, json.dumps(body, ensure_ascii=False).encode("utf-8")


def _error(message, code="ValidationError", status=400):
    return status, {"Content-Type": _CT, "x-amzn-errortype": code}, json.dumps({"Message": message}).encode()


def _group_not_found_message(name):
    arn = name if name.startswith("arn:") else sagemaker.feature_group_arn(name)
    return f"Resource Not Found: Amazon SageMaker can't find a FeatureGroup with name [{arn}]."


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------


def _parse_value(feature, ftype, value):
    """Return an error message if ``value`` does not parse as ``ftype``."""
    if ftype == "Integral":
        if not _INTEGRAL_RE.match(value) or not _INT64[0] <= int(value) <= _INT64[1]:
            return (f"Validation Error: Attempted to parse the feature value for the feature named [{feature}] "
                    "into a FeatureValue of type Integral. The provided value must be within the range of a "
                    "64 bit signed integer.")
    elif ftype == "Fractional":
        try:
            ok = math.isfinite(float(value))
        except ValueError:
            ok = False
        if not ok:
            return (f"Validation Error: Attempted to parse the feature value for the feature named [{feature}] "
                    "into a FeatureValue of type Fractional. The provided value must be within the range of a "
                    "double precision floating point number defined by the IEEE 754 standard. The input format "
                    "can be in either decimal form or scientific notation.")
    return None


def _event_seconds(group, value):
    """The event time in epoch seconds, or an error message."""
    feature = group["EventTimeFeatureName"]
    ftype = next(d["FeatureType"] for d in group["FeatureDefinitions"] if d["FeatureName"] == feature)
    if ftype == "Fractional":
        error = _parse_value(feature, "Fractional", value)
        return (None, error) if error else (float(value), None)
    if not _ISO_RE.match(value):
        return None, ("Validation Error: Unable to parse the value provided for the EventTime timestamp. "
                      "String EventTime FeatureValues must be an ISO-8601 string in the format(s) "
                      "[yyyy-MM-dd'T'HH:mm:ssZ, yyyy-MM-dd'T'HH:mm:ss.SSSZ].")
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ" if "." in value else "%Y-%m-%dT%H:%M:%SZ"
    return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).timestamp(), None


def _ttl_seconds(ttl):
    if not ttl:
        return None
    return _TTL_SECONDS.get(ttl.get("Unit"), 0) * int(ttl.get("Value", 0))


def _now():
    return datetime.now(timezone.utc).timestamp()


def _visible(entry):
    if entry is None or entry.get("Deleted"):
        return None
    if entry.get("ExpiresAt") is not None and entry["ExpiresAt"] <= _now():
        return None
    return entry


def _expires_at_text(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{int(round(seconds * 1000)) % 1000:03d}Z"


def _record_view(group, entry, feature_names, with_expiration):
    order = [d["FeatureName"] for d in group["FeatureDefinitions"]]
    wanted = set(feature_names) if feature_names else None
    record = [
        {
            "FeatureName": name,
            "ValueAsString": entry["Values"][name].get("ValueAsString"),
            "ValueAsStringList": entry["Values"][name].get("ValueAsStringList"),
        }
        for name in order
        if name in entry["Values"] and (wanted is None or name in wanted)
    ]
    expires = None
    if with_expiration and entry.get("ExpiresAt") is not None:
        expires = _expires_at_text(entry["ExpiresAt"])
    return expires, record


def _unknown_features_message(group, feature_names):
    known = {d["FeatureName"] for d in group["FeatureDefinitions"]}
    unknown = [n for n in feature_names or [] if n not in known]
    if unknown:
        return (f"Validation Error: Provided FeatureName(s) [{', '.join(unknown)}] doesn't exist in "
                f"FeatureGroup [{group['FeatureGroupName']}].")
    return None


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def _put_record(name, payload):
    group = sagemaker.lookup_feature_group(name)
    if group is None:
        return _error(_group_not_found_message(name))
    values = payload.get("Record") or []
    names = [v.get("FeatureName") for v in values]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        return _error(f"Validation Error: Provided values for Feature(s) [{', '.join(duplicates)}] multiple times. "
                      "Specify each feature value once per request.")
    definitions = {d["FeatureName"]: d for d in group["FeatureDefinitions"]}
    unknown = [n for n in names if n not in definitions]
    if unknown:
        return _error(f"Validation Error: Provided values for Feature(s) [{', '.join(unknown)}] which don't exist "
                      f"in FeatureGroup [{group['FeatureGroupName']}].")
    by_name = {v["FeatureName"]: v for v in values}
    for required in (group["RecordIdentifierFeatureName"], group["EventTimeFeatureName"]):
        if required not in by_name:
            return _error("Validation Error: Provided Record doesn't contain a required FeatureValue. "
                          f"Provide a FeatureValue with the name [{required}].")
    stored = {}
    for feature, value in by_name.items():
        definition = definitions[feature]
        if definition.get("CollectionType"):
            if value.get("ValueAsStringList") is None:
                return _error(f"Validation Error: ValueAsStringList required for feature [{feature}].")
            stored[feature] = {"ValueAsString": None, "ValueAsStringList": list(value["ValueAsStringList"])}
            continue
        if value.get("ValueAsString") is None:
            return _error(f"Validation Error: ValueAsString required for feature [{feature}].")
        error = _parse_value(feature, definition["FeatureType"], value["ValueAsString"])
        if error:
            return _error(error)
        stored[feature] = {"ValueAsString": value["ValueAsString"], "ValueAsStringList": None}
    record_id = stored[group["RecordIdentifierFeatureName"]]["ValueAsString"]
    if record_id == "":
        return _error("Validation Error: The RecordIdentifierFeatureValue can't be blank.")
    event_seconds, error = _event_seconds(group, stored[group["EventTimeFeatureName"]]["ValueAsString"])
    if error:
        return _error(error)

    online_enabled = bool((group.get("OnlineStoreConfig") or {}).get("EnableOnlineStore"))
    enabled = {"OnlineStore": online_enabled, "OfflineStore": bool(group.get("OfflineStoreConfig"))}
    targets = payload.get("TargetStores") or [s for s, on in enabled.items() if on]
    disabled = [s for s in targets if not enabled.get(s)]
    if disabled:
        return _error(f"Validation Error: Provided TargetStores: [{', '.join(disabled)}] aren't enabled.")
    if "OnlineStore" not in targets:
        return _ok()

    records = _records.setdefault(group["FeatureGroupName"], {})
    current = records.get(record_id)
    if current is not None and event_seconds < current["EventTime"]:
        return _ok()
    ttl = _ttl_seconds(payload.get("TtlDuration") or (group.get("OnlineStoreConfig") or {}).get("TtlDuration"))
    records[record_id] = {
        "EventTime": event_seconds,
        "Values": stored,
        "ExpiresAt": event_seconds + ttl if ttl else None,
    }
    return _ok()


def _get_record(name, query_params):
    group = sagemaker.lookup_feature_group(name)
    if group is None:
        return _error(_group_not_found_message(name))
    record_id = _query(query_params, "RecordIdentifierValueAsString")
    feature_names = _query_list(query_params, "FeatureName")
    error = _unknown_features_message(group, feature_names)
    if error:
        return _error(error)
    entry = _visible(_records.get(group["FeatureGroupName"], {}).get(record_id))
    if entry is None:
        return _ok({"ExpiresAt": None, "Record": None})
    expires, record = _record_view(group, entry, feature_names,
                                   _query(query_params, "ExpirationTimeResponse") == "Enabled")
    return _ok({"ExpiresAt": expires, "Record": record})


def _batch_get_record(payload):
    identifiers = payload.get("Identifiers") or []
    names = [i.get("FeatureGroupName") for i in identifiers]
    if len(set(names)) != len(names):
        return _error("Validation Error: identifier list contains at least two identifiers having same feature "
                      "group name. Remove duplicates and try again.")
    with_expiration = payload.get("ExpirationTimeResponse") == "Enabled"
    records, errors = [], []
    for identifier in identifiers:
        name = identifier.get("FeatureGroupName")
        ids = list(dict.fromkeys(identifier.get("RecordIdentifiersValueAsString") or []))
        group = sagemaker.lookup_feature_group(name)
        message = _group_not_found_message(name) if group is None else \
            _unknown_features_message(group, identifier.get("FeatureNames"))
        if message:
            errors.extend({"ErrorCode": "ValidationError", "ErrorMessage": message,
                           "FeatureGroupName": name, "RecordIdentifierValueAsString": rid} for rid in ids)
            continue
        stored = _records.get(group["FeatureGroupName"], {})
        for rid in ids:
            entry = _visible(stored.get(rid))
            if entry is None:
                continue
            expires, record = _record_view(group, entry, identifier.get("FeatureNames"), with_expiration)
            records.append({"ExpiresAt": expires, "FeatureGroupName": group["FeatureGroupName"],
                            "Record": record, "RecordIdentifierValueAsString": rid})
    return _ok({"Errors": errors, "Records": records, "UnprocessedIdentifiers": []})


def _delete_record(name, query_params):
    group = sagemaker.lookup_feature_group(name)
    if group is None:
        return _error(_group_not_found_message(name))
    record_id = _query(query_params, "RecordIdentifierValueAsString")
    event_seconds, error = _event_seconds(group, _query(query_params, "EventTime"))
    if error:
        return _error(error)
    targets = _query_list(query_params, "TargetStores")
    if targets and "OnlineStore" not in targets:
        return _ok()
    records = _records.get(group["FeatureGroupName"], {})
    if _query(query_params, "DeletionMode") == "HardDelete":
        records.pop(record_id, None)
        return _ok()
    current = records.get(record_id)
    if current is None or event_seconds >= current["EventTime"]:
        _records.setdefault(group["FeatureGroupName"], {})[record_id] = {
            "EventTime": event_seconds, "Values": {}, "ExpiresAt": None, "Deleted": True,
        }
    return _ok()


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def _query(query_params, key):
    value = query_params.get(key, "")
    if isinstance(value, list):
        value = value[0] if value else ""
    return value


def _query_list(query_params, key):
    value = query_params.get(key)
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


async def handle_request(method, path, headers, body, query_params):
    body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return _error("Could not parse request body into json.", code="SerializationException")

    if method == "POST" and path == "/BatchGetRecord":
        return _batch_get_record(payload)
    # The name may be a feature group ARN, which contains a slash.
    match = re.match(r"^/FeatureGroup/(.+)$", path)
    if match:
        name = unquote(match.group(1))
        if method == "PUT":
            return _put_record(name, payload)
        if method == "GET":
            return _get_record(name, query_params)
        if method == "DELETE":
            return _delete_record(name, query_params)
    logger.debug("featurestore-runtime: no route for %s %s (%s/%s)", method, path, get_account_id(), get_region())
    return _error(f"Operation {method} {path} not implemented", code="UnknownOperationException", status=404)
