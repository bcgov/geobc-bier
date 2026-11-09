import logging
import time
from arcgis.features import Feature

_log = logging.getLogger("feeds.wildfire.sync")

def sync_feature_layer(source_item, target_item, key_field="FIRE_NUMBER", max_retries=3, retry_delay=10):

    _log.info("Querying source and target layers...")

    key_field = key_field.upper()

    source_fl = source_item.layers[0]
    target_fl = target_item.layers[0]

    # Retry logic for source query
    source_features = None
    for attempt in range(1, max_retries + 1):
        source_features = source_fl.query(where="1=1", return_all_records=True).features
        if source_features:
            break
        if attempt < max_retries:
            _log.warning(f"Source returned 0 features on attempt {attempt}/{max_retries} — retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        else:
            _log.error(f"Source returned 0 features after {max_retries} attempts — aborting to prevent wipeout")
            raise ValueError(f"Source returned 0 features after {max_retries} attempts — aborting to prevent wipeout")

    target_features = target_fl.query(where="1=1", return_all_records=True).features
    print(f"Source features: {len(source_features)}, Target features: {len(target_features)}")

    # ----------------------------------------
    # Build lookup from target (stable layer)
    # ----------------------------------------
    target_lookup = {str(f.attributes[key_field]).strip(): f for f in target_features}

    adds = []
    updates = []
    deletes = []

    seen_keys = set()

    # ----------------------------------------
    # Process source features
    # ----------------------------------------
    for src in source_features:
        key = str(src.attributes[key_field]).strip()

        if not key:
            _log.warning("Skipping feature with no fire_number")
            continue

        seen_keys.add(key)

        if key in target_lookup:
            tgt = target_lookup[key]

            # ----------------------------------------
            # Only update if something changed
            # ----------------------------------------
            if _feature_changed(src, tgt):
                updates.append(_build_update_feature(src, tgt))
        else:
            adds.append(src)

    # ----------------------------------------
    # Detect deletes
    # ----------------------------------------
    for tgt in target_features:
        key = str(tgt.attributes[key_field]).strip()

        if key and key not in seen_keys:
            deletes.append(tgt.attributes["OBJECTID"])

    # ----------------------------------------
    # Apply edits
    # ----------------------------------------
    for f in adds:
        _log.info(f"Add: {f.attributes[key_field]}")
    for f in updates:
        _log.info(f"Update: {f.attributes[key_field]}")
    for oid in deletes:
        _log.info(f"Delete: {oid}")

    _log.info(
        f"Adds: {len(adds)}, Updates: {len(updates)}, Deletes: {len(deletes)}"
    )

    if adds:
        result_add = target_fl.edit_features(adds=adds)
        _log.debug(f"Add results: {result_add}")
    if updates:
        result_update = target_fl.edit_features(updates=updates)
        _log.debug(f"Update results: {result_update}")
    if deletes:
        result_delete = target_fl.edit_features(deletes=deletes)
        _log.debug(f"Delete results: {result_delete}")

    if not (adds or updates or deletes):
        _log.info("No changes detected — skipping edit_features")


# ----------------------------------------
# Change detection helper
# ----------------------------------------
def _feature_changed(src, tgt):
    """
    Compare attributes + geometry and print detailed differences
    """

    fire_number = src.attributes.get("FIRE_NUMBER")

    ignore_fields = {
        "OBJECTID",
        "GlobalID",
        "Shape__Area",
        "Shape__Length",
        "CreationDate",
        "Creator",
        "EditDate",
        "Editor",
    }

    def normalize(v):
        if v in ("", None):
            return None
        return str(v).strip()

    # ----------------------------------------
    # Attribute comparison (field-by-field)
    # ----------------------------------------
    changes = []

    all_keys = set(src.attributes.keys()) | set(tgt.attributes.keys())

    for k in all_keys:
        if k in ignore_fields:
            continue

        src_val = normalize(src.attributes.get(k))
        tgt_val = normalize(tgt.attributes.get(k))

        if src_val != tgt_val:
            changes.append((k, tgt_val, src_val))

    if changes:
        print(f"Attribute changes for {fire_number}:")
        for field, old, new in changes:
            print(f"  {field}: {old} -> {new}")
        return True

    # ----------------------------------------
    # Geometry comparison (point only)
    # ----------------------------------------
    def get_xy(g):
        if not g:
            return None, None
        return g.get("x"), g.get("y")

    x1, y1 = get_xy(src.geometry)
    x2, y2 = get_xy(tgt.geometry)

    if None not in (x1, y1, x2, y2):
        dx = abs(x1 - x2)
        dy = abs(y1 - y2)

        tol = 1e-5  # ~1 meter

        if dx > tol or dy > tol:
            print(f"  Geometry change for {fire_number}:")
            print(f"  From: ({x2}, {y2})")
            print(f"  To:   ({x1}, {y1})")
            print(f"  dx={dx}, dy={dy}")
            return True

    return False

def _build_update_feature(src, tgt):
    attrs = {}

    for k, v in src.attributes.items():
        if k in ("OBJECTID", "GlobalID"):
            continue
        attrs[k] = v

    # 🔥 critical: inject correct OBJECTID from target
    attrs["OBJECTID"] = tgt.attributes["OBJECTID"]

    return Feature(
        geometry=src.geometry,
        attributes=attrs
    )