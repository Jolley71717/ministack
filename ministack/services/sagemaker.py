# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Amazon SageMaker emulator: the Feature Store control plane.

JSON 1.1 protocol with X-Amz-Target prefix ``SageMaker``.

Implemented:
  CreateFeatureGroup, DescribeFeatureGroup, ListFeatureGroups,
  UpdateFeatureGroup, DeleteFeatureGroup, DescribeFeatureMetadata,
  UpdateFeatureMetadata, and AddTags, ListTags, DeleteTags on feature groups.

A feature group is ``Created`` as soon as CreateFeatureGroup returns and is
gone as soon as DeleteFeatureGroup returns; AWS passes through ``Creating``
and ``Deleting`` first. The offline store configuration is stored and
reported back, but no Parquet files are written and no Glue table is made.
Records live in ``sagemaker_featurestore_runtime``.
"""

import base64
import copy
import json
import logging
import re
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)

logger = logging.getLogger("sagemaker")

_CT = "application/x-amz-json-1.1"

_feature_groups = AccountRegionScopedDict()  # name -> feature group record

_NAME_PATTERN = "[a-zA-Z0-9]([_-]*[a-zA-Z0-9]){0,63}"
_NAME_RE = re.compile(rf"^{_NAME_PATTERN}$")
_RESERVED_FEATURE_NAMES = ("is_deleted", "write_time", "api_invocation_time")
_SORT_KEYS = {
    "Name": lambda g: g["FeatureGroupName"],
    "FeatureGroupStatus": lambda g: g["FeatureGroupStatus"],
    "OfflineStoreStatus": lambda g: (g.get("OfflineStoreStatus") or {}).get("Status", ""),
    "CreationTime": lambda g: g["CreationTime"],
}


def reset():
    _feature_groups.clear()


def get_state():
    return {"feature_groups": copy.deepcopy(_feature_groups)}


def load_persisted_state(data):
    if not data:
        return
    _feature_groups.clear()
    _feature_groups.update(data.get("feature_groups", {}))


# ---------------------------------------------------------------------------
# Helpers shared with the runtime
# ---------------------------------------------------------------------------


def feature_group_arn(name: str) -> str:
    return f"arn:aws:sagemaker:{get_region()}:{get_account_id()}:feature-group/{name}"


def lookup_feature_group(name_or_arn: str):
    """The feature group a name or ARN refers to in this account and region."""
    name = name_or_arn or ""
    if name.startswith("arn:"):
        try:
            spec = parse_arn(name)
        except ArnParseError:
            return None
        if (spec.service != "sagemaker" or spec.account_id != get_account_id()
                or spec.region != get_region() or not spec.resource.startswith("feature-group/")):
            return None
        name = spec.resource[len("feature-group/"):]
    return _feature_groups.get(name)


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------


def _ok(body=None):
    if body is None:
        return 200, {"Content-Type": _CT}, b""
    return json_response(body, content_type=_CT)


def _validation(message):
    return error_response_json("ValidationException", message, 400, content_type=_CT)


def _not_found(message):
    return error_response_json("ResourceNotFound", message, 400, message_key="Message", content_type=_CT)


def _group_not_found(name):
    return _not_found(f"Resource Not Found: Amazon SageMaker can't find a FeatureGroup with name {name}")


def _now():
    return round(time.time(), 3)


# ---------------------------------------------------------------------------
# Feature groups
# ---------------------------------------------------------------------------


def _validate_definitions(definitions, online_cfg):
    names = [d.get("FeatureName") for d in definitions]
    if len(set(names)) != len(names):
        return "Validation Error: FeatureDefinitions cannot have duplicate feature names."
    for name in names:
        if name in _RESERVED_FEATURE_NAMES:
            return (
                f"Validation Error: FeatureName: [{name}] is a reserved name of SageMaker FeatureStore. "
                "Please select another FeatureName and retry the request. Full list of SageMaker FeatureStore "
                f"reserved names: [{', '.join(_RESERVED_FEATURE_NAMES)}]"
            )
    if (online_cfg or {}).get("StorageType") != "InMemory":
        for d in definitions:
            if d.get("CollectionType"):
                return (f"Validation Error: CollectionType for Feature [{d.get('FeatureName')}] "
                        "is not supported by the specified StorageType.")
    return None


def _create_feature_group(payload):
    name = payload.get("FeatureGroupName") or ""
    if not _NAME_RE.match(name):
        return _validation(
            f"1 validation error detected: Value '{name}' at 'featureGroupName' failed to satisfy constraint: "
            f"Member must satisfy regular expression pattern: {_NAME_PATTERN}"
        )
    definitions = payload.get("FeatureDefinitions") or []
    online_cfg = payload.get("OnlineStoreConfig")
    offline_cfg = payload.get("OfflineStoreConfig")
    # AWS checks the role before the feature definitions.
    role_arn = payload.get("RoleArn")
    if offline_cfg:
        if not role_arn:
            return _validation("Validation Error: roleArn needs to be provided if OfflineStore is enabled.")
        try:
            role_account = parse_arn(role_arn).account_id
        except ArnParseError:
            role_account = None
        if role_account != get_account_id():
            return _validation("RoleArn: Cross-account pass role is not allowed.")
    error = _validate_definitions(definitions, online_cfg)
    if error:
        return _validation(error)
    types = {d.get("FeatureName"): d.get("FeatureType") for d in definitions}
    record_id = payload.get("RecordIdentifierFeatureName")
    event_time = payload.get("EventTimeFeatureName")
    if record_id not in types:
        return _validation("Validation Error: RecordIdentifierFeatureName is not found in names of FeatureDefinitions.")
    if event_time not in types:
        return _validation("Validation Error: EventTimeFeatureName is not found in names of FeatureDefinitions.")
    if types[event_time] not in ("Fractional", "String"):
        return _validation(
            f"Validation Error: invalid event time {event_time} feature type provided {types[event_time]}. "
            "Event time type should be either Fractional(Unix timestamp in seconds) or String (ISO-8601 format) type"
        )
    online_enabled = bool((online_cfg or {}).get("EnableOnlineStore"))
    if not online_enabled and not offline_cfg:
        return _validation(
            "Validation Error: OnlineStore and OfflineStore cannot be both disabled. "
            "Please enable at least one of them and retry the request."
        )
    if name in _feature_groups:
        return error_response_json(
            "ResourceInUse",
            f"Resource Already Exists: FeatureGroup with name {name} already exists. Choose a different name.",
            400, message_key="Message", content_type=_CT,
        )

    now = _now()
    group = {
        "CreationTime": now,
        "EventTimeFeatureName": event_time,
        "FeatureDefinitions": copy.deepcopy(definitions),
        "FeatureGroupArn": feature_group_arn(name),
        "FeatureGroupName": name,
        "FeatureGroupStatus": "Created",
        "RecordIdentifierFeatureName": record_id,
        "ThroughputConfig": {"ThroughputMode": "OnDemand"},
    }
    if payload.get("Description"):
        group["Description"] = payload["Description"]
    if online_cfg is not None:
        group["OnlineStoreConfig"] = copy.deepcopy(online_cfg)
    if online_enabled:
        group["OnlineStoreReadWriteType"] = "ReadWrite"
    if offline_cfg:
        group["OfflineStoreConfig"] = _resolve_offline_config(offline_cfg, name, now)
        group["RoleArn"] = role_arn
    if payload.get("ThroughputConfig"):
        group["ThroughputConfig"] = copy.deepcopy(payload["ThroughputConfig"])
    group["_Metadata"] = {
        d["FeatureName"]: {"CreationTime": now, "LastModifiedTime": now, "Parameters": {}} for d in definitions
    }
    group["_Tags"] = {t["Key"]: t.get("Value", "") for t in payload.get("Tags") or []}
    _feature_groups[name] = group
    return _ok({"FeatureGroupArn": group["FeatureGroupArn"]})


def _resolve_offline_config(offline_cfg, name, created):
    cfg = copy.deepcopy(offline_cfg)
    stamp = int(created)
    s3 = cfg.setdefault("S3StorageConfig", {})
    s3["ResolvedOutputS3Uri"] = (
        f"{s3.get('S3Uri', '').rstrip('/')}/{get_account_id()}/sagemaker/{get_region()}"
        f"/offline-store/{name}-{stamp}/data"
    )
    cfg.setdefault("DisableGlueTableCreation", False)
    if not cfg["DisableGlueTableCreation"] and "DataCatalogConfig" not in cfg:
        cfg["DataCatalogConfig"] = {
            "Catalog": "AwsDataCatalog",
            "Database": "sagemaker_featurestore",
            "TableName": f"{name.replace('-', '_')}_{stamp}",
        }
    return cfg


def _public(group):
    out = {k: copy.deepcopy(v) for k, v in group.items() if not k.startswith("_")}
    if group.get("OnlineStoreReadWriteType"):
        out["OnlineStoreReplicas"] = []
        out["OnlineStoreTotalItemCount"] = 0
        out["OnlineStoreTotalSizeBytes"] = 0
    return out


def _describe_feature_group(payload):
    group = lookup_feature_group(payload.get("FeatureGroupName"))
    if group is None:
        return _group_not_found(payload.get("FeatureGroupName"))
    return _ok(dict(sorted(_public(group).items())))


def encode_token(offset):
    return base64.b64encode(json.dumps({"offset": offset}).encode()).decode()


def decode_token(token):
    try:
        offset = json.loads(base64.b64decode(token, validate=True))["offset"]
    except (ValueError, KeyError, TypeError):
        return None
    return offset if isinstance(offset, int) and offset >= 0 else None


def _list_feature_groups(payload):
    groups = list(_feature_groups.values())
    if payload.get("NameContains"):
        groups = [g for g in groups if payload["NameContains"] in g["FeatureGroupName"]]
    if payload.get("FeatureGroupStatusEquals"):
        groups = [g for g in groups if g["FeatureGroupStatus"] == payload["FeatureGroupStatusEquals"]]
    if payload.get("OfflineStoreStatusEquals"):
        groups = [g for g in groups
                  if (g.get("OfflineStoreStatus") or {}).get("Status") == payload["OfflineStoreStatusEquals"]]
    if payload.get("CreationTimeAfter") is not None:
        groups = [g for g in groups if g["CreationTime"] > float(payload["CreationTimeAfter"])]
    if payload.get("CreationTimeBefore") is not None:
        groups = [g for g in groups if g["CreationTime"] < float(payload["CreationTimeBefore"])]
    groups.sort(key=_SORT_KEYS.get(payload.get("SortBy") or "CreationTime", _SORT_KEYS["CreationTime"]),
                reverse=(payload.get("SortOrder") or "Descending") == "Descending")

    start = 0
    if payload.get("NextToken"):
        start = decode_token(payload["NextToken"])
        if start is None:
            return _validation("Validation Error: Pagination token is invalid. Please provide a valid token.")
    limit = int(payload.get("MaxResults") or 10)
    page = groups[start:start + limit]
    body = {"FeatureGroupSummaries": [
        {k: g[k] for k in ("CreationTime", "FeatureGroupArn", "FeatureGroupName", "FeatureGroupStatus")}
        for g in page
    ]}
    if start + limit < len(groups):
        body["NextToken"] = encode_token(start + limit)
    return _ok(body)


def _update_feature_group(payload):
    group = lookup_feature_group(payload.get("FeatureGroupName"))
    if group is None:
        return _group_not_found(payload.get("FeatureGroupName"))
    additions = payload.get("FeatureAdditions") or []
    online_cfg = payload.get("OnlineStoreConfig")
    throughput = payload.get("ThroughputConfig")
    if not additions and online_cfg is None and throughput is None:
        return _validation("Validation Error: You must specify at least one action to update the FeatureGroup.")
    existing = {d["FeatureName"] for d in group["FeatureDefinitions"]}
    for d in additions:
        if d.get("FeatureName") in existing:
            return _validation(f"Validation Error: Feature [{d.get('FeatureName')}] already exists.")
    error = _validate_definitions(group["FeatureDefinitions"] + additions, group.get("OnlineStoreConfig"))
    if error:
        return _validation(error)

    now = _now()
    for d in additions:
        group["FeatureDefinitions"].append(copy.deepcopy(d))
        group["_Metadata"][d["FeatureName"]] = {"CreationTime": now, "LastModifiedTime": now, "Parameters": {}}
    if online_cfg is not None:
        store = group.setdefault("OnlineStoreConfig", {"EnableOnlineStore": True})
        for key in ("TtlDuration", "StorageType"):
            if key in online_cfg:
                store[key] = copy.deepcopy(online_cfg[key])
    if throughput is not None:
        group["ThroughputConfig"].update(copy.deepcopy(throughput))
    group["LastModifiedTime"] = now
    group["LastUpdateStatus"] = {"Status": "Successful"}
    return _ok({"FeatureGroupArn": group["FeatureGroupArn"]})


def _delete_feature_group(payload):
    group = lookup_feature_group(payload.get("FeatureGroupName"))
    if group is None:
        return _group_not_found(payload.get("FeatureGroupName"))
    del _feature_groups[group["FeatureGroupName"]]
    from ministack.services import sagemaker_featurestore_runtime

    sagemaker_featurestore_runtime.drop_records(group["FeatureGroupName"])
    return _ok()


# ---------------------------------------------------------------------------
# Feature metadata
# ---------------------------------------------------------------------------


def _feature(payload):
    group = lookup_feature_group(payload.get("FeatureGroupName"))
    if group is None:
        return None, None, _group_not_found(payload.get("FeatureGroupName"))
    feature = payload.get("FeatureName")
    definition = next((d for d in group["FeatureDefinitions"] if d["FeatureName"] == feature), None)
    if definition is None:
        return None, None, _not_found(
            f"Resource Not Found: Amazon SageMaker can't find a Feature with name {feature} "
            f"in FeatureGroup {group['FeatureGroupName']}."
        )
    return group, definition, None


def _describe_feature_metadata(payload):
    group, definition, error = _feature(payload)
    if error:
        return error
    meta = group["_Metadata"][definition["FeatureName"]]
    body = {
        "CreationTime": meta["CreationTime"],
        "FeatureGroupArn": group["FeatureGroupArn"],
        "FeatureGroupName": group["FeatureGroupName"],
        "FeatureName": definition["FeatureName"],
        "FeatureType": definition["FeatureType"],
        "LastModifiedTime": meta["LastModifiedTime"],
        "Parameters": [{"Key": k, "Value": v} for k, v in sorted(meta["Parameters"].items())],
    }
    if meta.get("Description"):
        body["Description"] = meta["Description"]
    return _ok(body)


def _update_feature_metadata(payload):
    group, definition, error = _feature(payload)
    if error:
        return error
    meta = group["_Metadata"][definition["FeatureName"]]
    if "Description" in payload:
        meta["Description"] = payload["Description"]
    for p in payload.get("ParameterAdditions") or []:
        meta["Parameters"][p["Key"]] = p.get("Value", "")
    for key in payload.get("ParameterRemovals") or []:
        meta["Parameters"].pop(key, None)
    meta["LastModifiedTime"] = _now()
    return _ok()


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def _tagged_group(arn):
    group = lookup_feature_group(arn) if (arn or "").startswith("arn:") else None
    if group is None:
        try:
            name = parse_arn(arn).resource.split("/", 1)[-1]
        except ArnParseError:
            name = arn
        return None, _validation(f"Resource Not Found: Amazon SageMaker can't find a FeatureGroup with name {name} ")
    return group, None


def _add_tags(payload):
    group, error = _tagged_group(payload.get("ResourceArn"))
    if error:
        return error
    tags = payload.get("Tags") or []
    for t in tags:
        group["_Tags"][t["Key"]] = t.get("Value", "")
    return _ok({"Tags": tags})


def _list_tags(payload):
    group, error = _tagged_group(payload.get("ResourceArn"))
    if error:
        return error
    return _ok({"Tags": [{"Key": k, "Value": v} for k, v in sorted(group["_Tags"].items())]})


def _delete_tags(payload):
    group, error = _tagged_group(payload.get("ResourceArn"))
    if error:
        return error
    for key in payload.get("TagKeys") or []:
        group["_Tags"].pop(key, None)
    return _ok({})


_DISPATCH = {
    "CreateFeatureGroup": _create_feature_group,
    "DescribeFeatureGroup": _describe_feature_group,
    "ListFeatureGroups": _list_feature_groups,
    "UpdateFeatureGroup": _update_feature_group,
    "DeleteFeatureGroup": _delete_feature_group,
    "DescribeFeatureMetadata": _describe_feature_metadata,
    "UpdateFeatureMetadata": _update_feature_metadata,
    "AddTags": _add_tags,
    "ListTags": _list_tags,
    "DeleteTags": _delete_tags,
}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("X-Amz-Target") or headers.get("x-amz-target") or ""
    action = target.split(".", 1)[1] if "." in target else target
    if not action:
        return error_response_json("InvalidAction", "missing X-Amz-Target", 400, content_type=_CT)
    body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "invalid JSON body", 400, content_type=_CT)
    fn = _DISPATCH.get(action)
    if fn is None:
        return error_response_json("InvalidAction", f"Operation '{action}' not implemented", 400, content_type=_CT)
    return fn(payload)
