import sys
import logging
from arcgis.gis import GIS

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from core.utils import _get_env
from feeds.wildfire_sync import sync_feature_layer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_log = logging.getLogger("runners.wildfire")

source_firelocation_item_id = "397a1defe7f04c2b8ef6511f6c087dbf" 
target_firelocation_item_id = "095dda13be424ebcb3756a24bbe6072c"

source_fireperim_item_id = "6ed3ec9b90f844fcaf9fea499bacae8e" 
target_fireperim_item_id = "a5ccbb7df33340368c27aba9e620df3a"

def main() -> None:
    portal_url = "https://bcgov03.maps.arcgis.com/"
    username = _get_env("AGO_USER")
    password = _get_env("AGO_PASS")

    _log.info("Connecting to ArcGIS Online: %s", portal_url)
    gis = GIS(portal_url, username, password)

    source_firelocation_item = gis.content.get(source_firelocation_item_id)
    if source_firelocation_item is None:
        _log.error("Could not find ArcGIS item: %s", source_firelocation_item_id)
        sys.exit(1)

    target_firelocation_item = gis.content.get(target_firelocation_item_id)
    if target_firelocation_item is None:
        _log.error("Could not find ArcGIS item: %s", target_firelocation_item_id)
        sys.exit(1)

    source_fireperim_item = gis.content.get(source_fireperim_item_id)
    if source_fireperim_item is None:
        _log.error("Could not find ArcGIS item: %s", source_fireperim_item_id)
        sys.exit(1)

    target_fireperim_item = gis.content.get(target_fireperim_item_id)
    if target_fireperim_item is None:
        _log.error("Could not find ArcGIS item: %s", target_fireperim_item_id)
        sys.exit(1)

    _log.info("Starting wildfire sync job")

    sync_feature_layer(
        source_item=source_firelocation_item,
        target_item=target_firelocation_item,
        key_field="fire_number",
    )

    sync_feature_layer(
        source_item=source_fireperim_item,
        target_item=target_fireperim_item,
        key_field="fire_number",
    )

if __name__ == "__main__":
    main()
