import json
import time
import statistics
import boto3
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session
from snowflake.ml.feature_store import FeatureStore, CreationMode
from snowflake.ml.feature_store import StoreType

SNOWFLAKE_SECRET_KEY = "TBD"
SNOWFLAKE_ROLE       = "TBD"
SNOWFLAKE_WAREHOUSE  = "TBD"
SNOWFLAKE_DATABASE   = "TBD"
SNOWFLAKE_SCHEMA     = "TBD"
FS_DATABASE          = "TBD"
FS_SCHEMA            = "TBD"
AWS_REGION           = "us-west-2"


def _get_secret():
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    resp = client.get_secret_value(SecretId=SNOWFLAKE_SECRET_KEY)
    return json.loads(resp["SecretString"])


def _private_key_bytes(pem: str) -> bytes:
    key = serialization.load_pem_private_key(
        pem.encode(), password=None, backend=default_backend()
    )
    return key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def handler(event, context):
    """
    Input event:
    {
        "entity_keys":    [["uuid-1"], ["uuid-2"], ...],
        "fv_name":        "DISPUTED_PAYMENTS_BY_UUID",
        "fv_version":     "v1",
        "warmup_seconds": 300,   // heat the warehouse for N seconds before measuring
        "n_runs":         50,    // number of measured requests after warmup
        "store_type":     "ONLINE"
    }

    Returns latency stats as JSON.
    """
    entity_keys     = event["entity_keys"]
    fv_name         = event.get("fv_name", "DISPUTED_PAYMENTS_BY_UUID")
    fv_version      = event.get("fv_version", "v1")
    warmup_seconds  = event.get("warmup_seconds", 300)  # default 5 min
    n_runs          = event.get("n_runs", 50)
    store_label     = event.get("store_type", "ONLINE")
    store_type      = StoreType.ONLINE if store_label == "ONLINE" else StoreType.OFFLINE

    creds = _get_secret()
    session = Session.builder.configs({
        "account":     creds["accountname"],
        "user":        creds["username"],
        "private_key": _private_key_bytes(creds["private_key_pem"]),
        "role":        SNOWFLAKE_ROLE,
        "warehouse":   SNOWFLAKE_WAREHOUSE,
        "database":    SNOWFLAKE_DATABASE,
        "schema":      SNOWFLAKE_SCHEMA,
    }).create()

    fs = FeatureStore(
        session=session,
        database=FS_DATABASE,
        name=FS_SCHEMA,
        default_warehouse=SNOWFLAKE_WAREHOUSE,
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )
    fv = fs.get_feature_view(fv_name, fv_version)

    # Time-based warmup: keep hitting the warehouse until it reaches steady state.
    # Snowflake vendor recommends 5–10 min of continuous traffic before measuring.
    print(f"Warming up warehouse for {warmup_seconds}s...")
    warmup_end = time.perf_counter() + warmup_seconds
    warmup_count = 0
    while time.perf_counter() < warmup_end:
        key = [entity_keys[warmup_count % len(entity_keys)]]
        fs.read_feature_view(fv, keys=key, store_type=store_type).collect()
        warmup_count += 1
    print(f"Warmup done — {warmup_count} requests fired. Starting measurement...")

    # Measured runs
    latencies = []
    for i in range(n_runs):
        key = [entity_keys[i % len(entity_keys)]]
        t0 = time.perf_counter()
        fs.read_feature_view(fv, keys=key, store_type=store_type).collect()
        latencies.append((time.perf_counter() - t0) * 1000)

    session.close()

    def pct(data, p):
        s = sorted(data)
        idx = (len(s) - 1) * p / 100
        lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
        return round(s[lo] + (s[hi] - s[lo]) * (idx - lo), 1)

    return {
        "store_type":     store_label,
        "fv_name":        fv_name,
        "fv_version":     fv_version,
        "warmup_seconds": warmup_seconds,
        "warmup_requests": warmup_count,
        "n":              len(latencies),
        "min_ms":     round(min(latencies), 1),
        "p50_ms":     pct(latencies, 50),
        "p95_ms":     pct(latencies, 95),
        "p99_ms":     pct(latencies, 99),
        "max_ms":     round(max(latencies), 1),
        "mean_ms":    round(statistics.mean(latencies), 1),
    }
