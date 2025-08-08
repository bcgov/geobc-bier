from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import os

# --- Configuration ---
source_url = "https://services6.arcgis.com/ubm4tcTYICKBpist/arcgis/rest/services/BCWS_ActiveFires_PublicView/FeatureServer/0"
target_url = "https://services1.arcgis.com/xeMpV7tU1t4KD3Ei/arcgis/rest/services/Fire_Locations_Test/FeatureServer/0"

# --- Authentication ---
username = os.getenv("AGO_USER")
password = os.getenv("AGO_PASS")
gis = GIS("https://www.arcgis.com", username, password)

# --- Connect to Feature Layers ---
source_layer = FeatureLayer(source_url, gis=gis)
target_layer = FeatureLayer(target_url, gis=gis)

# --- Attempt to truncate the target layer ---
print("Attempting to truncate target layer...")
try:
    truncate_result = target_layer.manager.truncate()
    print("Successfully truncated target layer.")
except Exception as e:
    print(f"Truncate failed: {e}")
    print("Attempting to delete all features instead...")

    # Fall back to deleting all features if truncate is not available
    try:
        delete_result = target_layer.delete_features(where="1=1")
        deleted = delete_result.get('deleteResults', [])
        success_count = sum(1 for r in deleted if r.get('success'))
        fail_count = len(deleted) - success_count
        print(f"Deleted {success_count} features from target layer.")
        if fail_count > 0:
            print(f"{fail_count} features failed to delete.")
    except Exception as delete_error:
        print(f"Failed to delete features: {delete_error}")
        exit(1)

# --- Query all features from source ---
print("Querying source features...")
source_features = source_layer.query(where="1=1", out_fields="*", return_geometry=True).features

if not source_features:
    print("No features found in source layer.")
else:
    print(f"Found {len(source_features)} features. Adding to target layer...")

    # Add features to the target layer
    add_result = target_layer.edit_features(adds=source_features)

    # Check results
    if 'addResults' in add_result:
        success_count = sum(1 for r in add_result['addResults'] if r.get('success'))
        fail_count = len(add_result['addResults']) - success_count
        print(f"Successfully added {success_count} features.")
        if fail_count > 0:
            print(f"Failed to add {fail_count} features.")
    else:
        print("No addResults returned. Something went wrong.")