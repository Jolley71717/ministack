"""SageMaker Feature Store: feature groups (sagemaker) and the online store
(sagemaker-featurestore-runtime).

Expected values come from the same calls recorded against AWS in us-east-1.
"""

import json
import time
import uuid

import pytest
from botocore.exceptions import ClientError


def _name(prefix="fs-test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


_DEFS = [
    {"FeatureName": "customer_id", "FeatureType": "String"},
    {"FeatureName": "event_time", "FeatureType": "Fractional"},
    {"FeatureName": "age", "FeatureType": "Integral"},
    {"FeatureName": "score", "FeatureType": "Fractional"},
]


def _create(sagemaker, name=None, **overrides):
    name = name or _name()
    params = {
        "FeatureGroupName": name,
        "RecordIdentifierFeatureName": "customer_id",
        "EventTimeFeatureName": "event_time",
        "FeatureDefinitions": _DEFS,
        "OnlineStoreConfig": {"EnableOnlineStore": True},
    }
    params.update(overrides)
    sagemaker.create_feature_group(**params)
    return name


def _rec(cid, t, **features):
    record = [{"FeatureName": "customer_id", "ValueAsString": cid},
              {"FeatureName": "event_time", "ValueAsString": t}]
    record += [{"FeatureName": k, "ValueAsString": v} for k, v in features.items()]
    return record


def _values(response):
    return {f["FeatureName"]: f["ValueAsString"] for f in response.get("Record") or []}


def _error(exc):
    return exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]


# -- feature groups ------------------------------------------------------------


def test_sagemaker_create_and_describe_feature_group(sagemaker):
    name = _create(sagemaker, Description="probe group", Tags=[{"Key": "team", "Value": "ml"}])
    group = sagemaker.describe_feature_group(FeatureGroupName=name)
    assert group["FeatureGroupArn"].endswith(f":feature-group/{name}")
    assert group["FeatureGroupStatus"] == "Created"
    assert group["Description"] == "probe group"
    assert group["FeatureDefinitions"] == _DEFS
    assert group["OnlineStoreConfig"] == {"EnableOnlineStore": True}
    assert group["ThroughputConfig"] == {"ThroughputMode": "OnDemand"}
    assert "LastModifiedTime" not in group and "OfflineStoreConfig" not in group

    with pytest.raises(ClientError) as exc:
        _create(sagemaker, name)
    assert _error(exc) == ("ResourceInUse", f"Resource Already Exists: FeatureGroup with name {name} already "
                                            "exists. Choose a different name.")


@pytest.mark.parametrize("overrides,message", [
    ({"EventTimeFeatureName": "nope"},
     "Validation Error: EventTimeFeatureName is not found in names of FeatureDefinitions."),
    ({"RecordIdentifierFeatureName": "nope"},
     "Validation Error: RecordIdentifierFeatureName is not found in names of FeatureDefinitions."),
    ({"OnlineStoreConfig": {"EnableOnlineStore": False}},
     "Validation Error: OnlineStore and OfflineStore cannot be both disabled. Please enable at least one of them "
     "and retry the request."),
    ({"EventTimeFeatureName": "age"},
     "Validation Error: invalid event time age feature type provided Integral. Event time type should be either "
     "Fractional(Unix timestamp in seconds) or String (ISO-8601 format) type"),
    ({"FeatureDefinitions": _DEFS + [{"FeatureName": "age", "FeatureType": "String"}]},
     "Validation Error: FeatureDefinitions cannot have duplicate feature names."),
    ({"FeatureDefinitions": _DEFS + [{"FeatureName": "write_time", "FeatureType": "String"}]},
     "Validation Error: FeatureName: [write_time] is a reserved name of SageMaker FeatureStore. Please select "
     "another FeatureName and retry the request. Full list of SageMaker FeatureStore reserved names: "
     "[is_deleted, write_time, api_invocation_time]"),
    ({"FeatureDefinitions": _DEFS + [{"FeatureName": "tags", "FeatureType": "String", "CollectionType": "List"}]},
     "Validation Error: CollectionType for Feature [tags] is not supported by the specified StorageType."),
    ({"OfflineStoreConfig": {"S3StorageConfig": {"S3Uri": "s3://bucket/x"}}},
     "Validation Error: roleArn needs to be provided if OfflineStore is enabled."),
    ({"OfflineStoreConfig": {"S3StorageConfig": {"S3Uri": "s3://bucket/x"}},
      "RoleArn": "arn:aws:iam::123456789012:role/x",
      "FeatureDefinitions": _DEFS + [{"FeatureName": "tags", "FeatureType": "String", "CollectionType": "List"}]},
     "RoleArn: Cross-account pass role is not allowed."),
], ids=["event-time", "record-id", "no-store", "integral-event-time", "duplicate", "reserved", "collection",
        "offline-no-role", "cross-account-role"])
def test_sagemaker_create_feature_group_validation(sagemaker, overrides, message):
    with pytest.raises(ClientError) as exc:
        _create(sagemaker, **overrides)
    assert _error(exc) == ("ValidationException", message)


def test_sagemaker_create_feature_group_name_pattern(sagemaker):
    with pytest.raises(ClientError) as exc:
        _create(sagemaker, "bad_name!")
    assert _error(exc) == (
        "ValidationException",
        "1 validation error detected: Value 'bad_name!' at 'featureGroupName' failed to satisfy constraint: "
        "Member must satisfy regular expression pattern: [a-zA-Z0-9]([_-]*[a-zA-Z0-9]){0,63}")


def test_sagemaker_offline_store_config_is_resolved(sagemaker):
    role = "arn:aws:iam::000000000000:role/fs-role"
    name = _create(sagemaker, OfflineStoreConfig={"S3StorageConfig": {"S3Uri": "s3://fs-bucket/offline"}},
                   RoleArn=role)
    group = sagemaker.describe_feature_group(FeatureGroupName=name)
    stamp = int(group["CreationTime"].timestamp())
    assert group["RoleArn"] == role
    assert group["OfflineStoreConfig"] == {
        "S3StorageConfig": {
            "S3Uri": "s3://fs-bucket/offline",
            "ResolvedOutputS3Uri": f"s3://fs-bucket/offline/000000000000/sagemaker/us-east-1/offline-store/"
                                   f"{name}-{stamp}/data",
        },
        "DisableGlueTableCreation": False,
        "DataCatalogConfig": {"Catalog": "AwsDataCatalog", "Database": "sagemaker_featurestore",
                              "TableName": f"{name.replace('-', '_')}_{stamp}"},
    }


def test_sagemaker_list_feature_groups(sagemaker):
    prefix = _name("fs-list")
    first = _create(sagemaker, f"{prefix}-a")
    time.sleep(0.01)
    second = _create(sagemaker, f"{prefix}-b")
    listed = sagemaker.list_feature_groups(NameContains=prefix)["FeatureGroupSummaries"]
    assert [g["FeatureGroupName"] for g in listed] == [second, first]
    assert set(listed[0]) == {"CreationTime", "FeatureGroupArn", "FeatureGroupName", "FeatureGroupStatus"}

    by_name = sagemaker.list_feature_groups(NameContains=prefix, SortBy="Name", SortOrder="Ascending")
    assert [g["FeatureGroupName"] for g in by_name["FeatureGroupSummaries"]] == [first, second]

    page = sagemaker.list_feature_groups(NameContains=prefix, MaxResults=1)
    assert [g["FeatureGroupName"] for g in page["FeatureGroupSummaries"]] == [second]
    rest = sagemaker.list_feature_groups(NameContains=prefix, MaxResults=1, NextToken=page["NextToken"])
    assert [g["FeatureGroupName"] for g in rest["FeatureGroupSummaries"]] == [first]
    assert "NextToken" not in rest

    with pytest.raises(ClientError) as exc:
        sagemaker.list_feature_groups(NextToken="garbage")
    assert _error(exc) == ("ValidationException",
                           "Validation Error: Pagination token is invalid. Please provide a valid token.")


def test_sagemaker_update_feature_group(sagemaker, featurestore):
    name = _create(sagemaker)
    sagemaker.update_feature_group(FeatureGroupName=name,
                                   FeatureAdditions=[{"FeatureName": "city", "FeatureType": "String"}])
    group = sagemaker.describe_feature_group(FeatureGroupName=name)
    assert group["FeatureDefinitions"][-1] == {"FeatureName": "city", "FeatureType": "String"}
    assert group["LastUpdateStatus"] == {"Status": "Successful"}
    assert "LastModifiedTime" in group
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c7", "1700000000.0", city="Helena"))
    assert _values(featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c7"))["city"] \
        == "Helena"

    sagemaker.update_feature_group(FeatureGroupName=name, OnlineStoreConfig={"TtlDuration": {"Unit": "Hours", "Value": 2}})
    assert sagemaker.describe_feature_group(FeatureGroupName=name)["OnlineStoreConfig"] == {
        "EnableOnlineStore": True, "TtlDuration": {"Unit": "Hours", "Value": 2}}

    with pytest.raises(ClientError) as exc:
        sagemaker.update_feature_group(FeatureGroupName=name,
                                       FeatureAdditions=[{"FeatureName": "age", "FeatureType": "Integral"}])
    assert _error(exc) == ("ValidationException", "Validation Error: Feature [age] already exists.")
    with pytest.raises(ClientError) as exc:
        sagemaker.update_feature_group(FeatureGroupName=name)
    assert _error(exc) == ("ValidationException",
                           "Validation Error: You must specify at least one action to update the FeatureGroup.")


def test_sagemaker_delete_feature_group(sagemaker, featurestore):
    name = _create(sagemaker)
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c1", "1700000000.0"))
    sagemaker.delete_feature_group(FeatureGroupName=name)
    for call in (lambda: sagemaker.describe_feature_group(FeatureGroupName=name),
                 lambda: sagemaker.delete_feature_group(FeatureGroupName=name)):
        with pytest.raises(ClientError) as exc:
            call()
        assert _error(exc) == ("ResourceNotFound",
                               f"Resource Not Found: Amazon SageMaker can't find a FeatureGroup with name {name}")
    # A group created again under the same name starts empty.
    _create(sagemaker, name)
    assert featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c1").get("Record") is None


def test_sagemaker_feature_metadata(sagemaker):
    name = _create(sagemaker)
    meta = sagemaker.describe_feature_metadata(FeatureGroupName=name, FeatureName="age")
    assert (meta["FeatureType"], meta["Parameters"], "Description" in meta) == ("Integral", [], False)
    sagemaker.update_feature_metadata(FeatureGroupName=name, FeatureName="age", Description="customer age",
                                      ParameterAdditions=[{"Key": "unit", "Value": "years"},
                                                          {"Key": "pii", "Value": "no"}])
    meta = sagemaker.describe_feature_metadata(FeatureGroupName=name, FeatureName="age")
    assert meta["Description"] == "customer age"
    assert meta["Parameters"] == [{"Key": "pii", "Value": "no"}, {"Key": "unit", "Value": "years"}]
    sagemaker.update_feature_metadata(FeatureGroupName=name, FeatureName="age", ParameterRemovals=["pii"])
    assert sagemaker.describe_feature_metadata(FeatureGroupName=name, FeatureName="age")["Parameters"] == [
        {"Key": "unit", "Value": "years"}]

    with pytest.raises(ClientError) as exc:
        sagemaker.describe_feature_metadata(FeatureGroupName=name, FeatureName="nope")
    assert _error(exc) == ("ResourceNotFound", "Resource Not Found: Amazon SageMaker can't find a Feature with "
                                               f"name nope in FeatureGroup {name}.")


def test_sagemaker_feature_group_tags(sagemaker):
    name = _create(sagemaker, Tags=[{"Key": "team", "Value": "ml"}])
    arn = sagemaker.describe_feature_group(FeatureGroupName=name)["FeatureGroupArn"]
    assert sagemaker.add_tags(ResourceArn=arn, Tags=[{"Key": "env", "Value": "dev"}])["Tags"] == [
        {"Key": "env", "Value": "dev"}]
    assert sagemaker.list_tags(ResourceArn=arn)["Tags"] == [{"Key": "env", "Value": "dev"},
                                                            {"Key": "team", "Value": "ml"}]
    sagemaker.delete_tags(ResourceArn=arn, TagKeys=["team"])
    assert sagemaker.list_tags(ResourceArn=arn)["Tags"] == [{"Key": "env", "Value": "dev"}]

    with pytest.raises(ClientError) as exc:
        sagemaker.list_tags(ResourceArn=arn.replace(name, "fs-test-absent"))
    assert _error(exc) == ("ValidationException",
                           "Resource Not Found: Amazon SageMaker can't find a FeatureGroup with name fs-test-absent ")


# -- records -----------------------------------------------------------------------


def test_featurestore_keeps_latest_record_by_event_time(sagemaker, featurestore):
    name = _create(sagemaker)
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c1", "1700000000.0", age="30", score="0.5"))
    got = featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c1")
    assert got["Record"] == [
        {"FeatureName": "customer_id", "ValueAsString": "c1"},
        {"FeatureName": "event_time", "ValueAsString": "1700000000.0"},
        {"FeatureName": "age", "ValueAsString": "30"},
        {"FeatureName": "score", "ValueAsString": "0.5"},
    ]
    assert _values(featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c1",
                                           FeatureNames=["age"])) == {"age": "30"}

    # An older write is ignored; the same or a newer event time replaces the whole record.
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c1", "1600000000.0", age="99"))
    assert _values(featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c1"))["age"] == "30"
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c1", "1700000000.0", age="31"))
    assert _values(featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c1")) == {
        "customer_id": "c1", "event_time": "1700000000.0", "age": "31"}
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c1", "1800000000.0", age="40"))
    assert _values(featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c1")) == {
        "customer_id": "c1", "event_time": "1800000000.0", "age": "40"}

    assert "Record" not in featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="zzz")


def test_featurestore_raw_get_record_shape(sagemaker, featurestore):
    """AWS sends ExpiresAt and ValueAsStringList as explicit nulls."""
    name = _create(sagemaker)
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c1", "1700000000.0"))
    seen = {}
    featurestore.meta.events.register(
        "after-call.sagemaker-featurestore-runtime.GetRecord",
        lambda http_response, **_: seen.update(body=http_response.content,
                                                 ct=http_response.headers.get("content-type")))
    featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c1")
    assert seen["ct"] == "application/json"
    assert json.loads(seen["body"]) == {"ExpiresAt": None, "Record": [
        {"FeatureName": "customer_id", "ValueAsString": "c1", "ValueAsStringList": None},
        {"FeatureName": "event_time", "ValueAsString": "1700000000.0", "ValueAsStringList": None}]}
    featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="absent")
    assert json.loads(seen["body"]) == {"ExpiresAt": None, "Record": None}


@pytest.mark.parametrize("record,message", [
    (_rec("c3", "1700000000.0") + [{"FeatureName": "nope", "ValueAsString": "x"}],
     "Validation Error: Provided values for Feature(s) [nope] which don't exist in FeatureGroup [{name}]."),
    ([{"FeatureName": "event_time", "ValueAsString": "1700000000.0"}],
     "Validation Error: Provided Record doesn't contain a required FeatureValue. Provide a FeatureValue with the "
     "name [customer_id]."),
    ([{"FeatureName": "customer_id", "ValueAsString": "c3"}],
     "Validation Error: Provided Record doesn't contain a required FeatureValue. Provide a FeatureValue with the "
     "name [event_time]."),
    (_rec("c3", "1700000000.0", age="abc"),
     "Validation Error: Attempted to parse the feature value for the feature named [age] into a FeatureValue of "
     "type Integral. The provided value must be within the range of a 64 bit signed integer."),
    (_rec("c3", "1700000000.0", age="1.0"),
     "Validation Error: Attempted to parse the feature value for the feature named [age] into a FeatureValue of "
     "type Integral. The provided value must be within the range of a 64 bit signed integer."),
    (_rec("c3", "2023-11-14T22:13:20Z"),
     "Validation Error: Attempted to parse the feature value for the feature named [event_time] into a "
     "FeatureValue of type Fractional. The provided value must be within the range of a double precision floating "
     "point number defined by the IEEE 754 standard. The input format can be in either decimal form or scientific "
     "notation."),
    (_rec("c3", "1700000000.0", age="1") + [{"FeatureName": "age", "ValueAsString": "2"}],
     "Validation Error: Provided values for Feature(s) [age] multiple times. Specify each feature value once per "
     "request."),
    (_rec("", "1700000000.0"), "Validation Error: The RecordIdentifierFeatureValue can't be blank."),
    (_rec("c3", "1700000000.0") + [{"FeatureName": "age", "ValueAsStringList": ["1"]}],
     "Validation Error: ValueAsString required for feature [age]."),
], ids=["unknown", "no-id", "no-time", "integral", "integral-decimal", "iso-on-fractional", "duplicate",
        "blank-id", "list-on-scalar"])
def test_featurestore_put_record_validation(sagemaker, featurestore, record, message):
    name = _create(sagemaker)
    with pytest.raises(ClientError) as exc:
        featurestore.put_record(FeatureGroupName=name, Record=record)
    assert _error(exc) == ("ValidationError", message.format(name=name))
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_featurestore_iso_event_time(sagemaker, featurestore):
    name = _create(sagemaker, FeatureDefinitions=[
        {"FeatureName": "customer_id", "FeatureType": "String"},
        {"FeatureName": "event_time", "FeatureType": "String"}])
    featurestore.put_record(FeatureGroupName=name, Record=_rec("a", "2023-11-14T22:13:20Z"))
    featurestore.put_record(FeatureGroupName=name, Record=_rec("b", "2023-11-14T22:13:20.123Z"))
    for value in ("2023-11-14T22:13:20", "2023-11-14", "1700000000"):
        with pytest.raises(ClientError) as exc:
            featurestore.put_record(FeatureGroupName=name, Record=_rec("c", value))
        assert _error(exc) == ("ValidationError",
                               "Validation Error: Unable to parse the value provided for the EventTime timestamp. "
                               "String EventTime FeatureValues must be an ISO-8601 string in the format(s) "
                               "[yyyy-MM-dd'T'HH:mm:ssZ, yyyy-MM-dd'T'HH:mm:ss.SSSZ].")
    # Newer by event time, not by write order.
    featurestore.put_record(FeatureGroupName=name, Record=_rec("a", "2023-11-14T22:13:19.999Z"))
    assert _values(featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="a"))[
        "event_time"] == "2023-11-14T22:13:20Z"


def test_featurestore_missing_group_and_feature(sagemaker, featurestore):
    account = "000000000000"
    message = ("Resource Not Found: Amazon SageMaker can't find a FeatureGroup with name "
               f"[arn:aws:sagemaker:us-east-1:{account}:feature-group/fs-test-absent].")
    for call in (
        lambda: featurestore.put_record(FeatureGroupName="fs-test-absent", Record=_rec("a", "1700000000.0")),
        lambda: featurestore.get_record(FeatureGroupName="fs-test-absent", RecordIdentifierValueAsString="a"),
        lambda: featurestore.delete_record(FeatureGroupName="fs-test-absent", RecordIdentifierValueAsString="a",
                                           EventTime="1700000000.0"),
    ):
        with pytest.raises(ClientError) as exc:
            call()
        assert _error(exc) == ("ValidationError", message)

    name = _create(sagemaker)
    with pytest.raises(ClientError) as exc:
        featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="a", FeatureNames=["zz"])
    assert _error(exc) == ("ValidationError",
                           f"Validation Error: Provided FeatureName(s) [zz] doesn't exist in FeatureGroup [{name}].")


def test_featurestore_get_record_by_arn(sagemaker, featurestore):
    name = _create(sagemaker)
    arn = sagemaker.describe_feature_group(FeatureGroupName=name)["FeatureGroupArn"]
    featurestore.put_record(FeatureGroupName=arn, Record=_rec("a", "1700000000.0"))
    assert _values(featurestore.get_record(FeatureGroupName=arn, RecordIdentifierValueAsString="a"))[
        "customer_id"] == "a"


def test_featurestore_target_stores(sagemaker, featurestore):
    name = _create(sagemaker)
    with pytest.raises(ClientError) as exc:
        featurestore.put_record(FeatureGroupName=name, Record=_rec("c6", "1700000000.0"),
                                TargetStores=["OfflineStore"])
    assert _error(exc) == ("ValidationError", "Validation Error: Provided TargetStores: [OfflineStore] aren't enabled.")

    offline = _create(sagemaker, OfflineStoreConfig={"S3StorageConfig": {"S3Uri": "s3://fs-bucket/o"}},
                      RoleArn="arn:aws:iam::000000000000:role/fs-role")
    featurestore.put_record(FeatureGroupName=offline, Record=_rec("o2", "1700000000.0"),
                            TargetStores=["OfflineStore"])
    assert "Record" not in featurestore.get_record(FeatureGroupName=offline, RecordIdentifierValueAsString="o2")


def test_featurestore_delete_record(sagemaker, featurestore):
    name = _create(sagemaker)
    # A soft delete leaves a marker at its event time, even for a record that was never written.
    featurestore.delete_record(FeatureGroupName=name, RecordIdentifierValueAsString="c2", EventTime="1800000000.0")
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c2", "1750000000.0", age="21"))
    assert "Record" not in featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c2")
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c2", "1900000000.0", age="22"))
    assert _values(featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c2"))["age"] == "22"

    # A hard delete forgets the record, so an older write shows again.
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c5", "1700000000.0", age="6"))
    featurestore.delete_record(FeatureGroupName=name, RecordIdentifierValueAsString="c5", EventTime="1700000000.0",
                               DeletionMode="HardDelete")
    assert "Record" not in featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c5")
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c5", "1600000000.0", age="1"))
    assert _values(featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="c5"))["age"] == "1"

    with pytest.raises(ClientError) as exc:
        featurestore.delete_record(FeatureGroupName=name, RecordIdentifierValueAsString="c1", EventTime="nope")
    assert _error(exc)[0] == "ValidationError"


def test_featurestore_ttl(sagemaker, featurestore):
    name = _create(sagemaker, OnlineStoreConfig={"EnableOnlineStore": True,
                                                 "TtlDuration": {"Unit": "Minutes", "Value": 30}})
    now = int(time.time())
    featurestore.put_record(FeatureGroupName=name, Record=_rec("t1", f"{now}.0"))
    assert featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="t1").get("ExpiresAt") is None
    got = featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="t1",
                                  ExpirationTimeResponse="Enabled")
    assert got["ExpiresAt"] == time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now + 1800))

    featurestore.put_record(FeatureGroupName=name, Record=_rec("t2", f"{now}.0"),
                            TtlDuration={"Unit": "Hours", "Value": 3})
    got = featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="t2",
                                  ExpirationTimeResponse="Enabled")
    assert got["ExpiresAt"] == time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now + 3 * 3600))

    # The TTL counts from the event time, so an old event time is already expired.
    featurestore.put_record(FeatureGroupName=name, Record=_rec("t3", "1577836800.0"))
    assert "Record" not in featurestore.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="t3")


def test_featurestore_batch_get_record(sagemaker, featurestore):
    name = _create(sagemaker)
    other = _create(sagemaker)
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c1", "1700000000.0", age="40"))
    featurestore.put_record(FeatureGroupName=name, Record=_rec("c5", "1700000000.0", age="6"))
    featurestore.put_record(FeatureGroupName=other, Record=_rec("o1", "1700000000.0"))

    got = featurestore.batch_get_record(Identifiers=[
        {"FeatureGroupName": name, "RecordIdentifiersValueAsString": ["c1", "c1", "zzz", "c5"],
         "FeatureNames": ["age"]},
        {"FeatureGroupName": other, "RecordIdentifiersValueAsString": ["o1"]},
    ])
    assert got["Errors"] == [] and got["UnprocessedIdentifiers"] == []
    assert sorted((r["FeatureGroupName"], r["RecordIdentifierValueAsString"], tuple(_values(r).items()))
                  for r in got["Records"]) == sorted([
        (name, "c1", (("age", "40"),)), (name, "c5", (("age", "6"),)),
        (other, "o1", (("customer_id", "o1"), ("event_time", "1700000000.0")))])

    got = featurestore.batch_get_record(Identifiers=[
        {"FeatureGroupName": "fs-test-absent", "RecordIdentifiersValueAsString": ["c1"]},
        {"FeatureGroupName": name, "RecordIdentifiersValueAsString": ["c1"], "FeatureNames": ["zz"]},
    ])
    assert got["Records"] == []
    assert [(e["FeatureGroupName"], e["ErrorCode"], e["ErrorMessage"]) for e in got["Errors"]] == [
        ("fs-test-absent", "ValidationError", "Resource Not Found: Amazon SageMaker can't find a FeatureGroup with "
         "name [arn:aws:sagemaker:us-east-1:000000000000:feature-group/fs-test-absent]."),
        (name, "ValidationError",
         f"Validation Error: Provided FeatureName(s) [zz] doesn't exist in FeatureGroup [{name}]."),
    ]

    with pytest.raises(ClientError) as exc:
        featurestore.batch_get_record(Identifiers=[
            {"FeatureGroupName": name, "RecordIdentifiersValueAsString": ["c1"]},
            {"FeatureGroupName": name, "RecordIdentifiersValueAsString": ["c5"]}])
    assert _error(exc) == ("ValidationError", "Validation Error: identifier list contains at least two identifiers "
                                              "having same feature group name. Remove duplicates and try again.")


# -- authorization ---------------------------------------------------------------


@pytest.mark.auth_contract
def test_sagemaker_feature_store_authorization_contract(auth_enabled, iam_user, sagemaker):
    name = _create(sagemaker)
    arn = sagemaker.describe_feature_group(FeatureGroupName=name)["FeatureGroupArn"]
    allowed = iam_user(policy={"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Resource": arn,
        "Action": ["sagemaker:DescribeFeatureGroup", "sagemaker:PutRecord", "sagemaker:GetRecord"]}]})
    nobody = iam_user()

    allowed.client("sagemaker").describe_feature_group(FeatureGroupName=name)
    runtime = allowed.client("sagemaker-featurestore-runtime")
    runtime.put_record(FeatureGroupName=name, Record=_rec("a", "1700000000.0"))
    assert _values(runtime.get_record(FeatureGroupName=name, RecordIdentifierValueAsString="a"))["customer_id"] == "a"

    calls = (
        lambda: nobody.client("sagemaker").describe_feature_group(FeatureGroupName=name),
        lambda: nobody.client("sagemaker-featurestore-runtime").get_record(
            FeatureGroupName=name, RecordIdentifierValueAsString="a"),
    )
    for call in calls:
        if not auth_enabled:
            call()
            continue
        with pytest.raises(ClientError) as exc:
            call()
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
