"""
Core API module for managing ArcGIS Online (AGO) feature layers.

Provides functions to delete/truncate and append features to layers.
"""


import logging

_log = logging.getLogger("core.ago")

def delete_and_truncate(ago_item, layer_num: int = 0) -> None:
    """
    Delete all features from the specified layer and truncate it.

    Args:
        ago_item: The AGO item containing the layers.
        layer_num (int): The index of the layer to operate on (default 0).
    """
    layer = ago_item.layers[layer_num]

    count = layer.query(return_count_only=True)
    layer.delete_features(where="1=1")
    layer.manager.truncate()

    _log.info(f"Deleted and truncated {count} features from {ago_item.itemid}")


def append_features(ago_item, features_list, layer_num: int = 0) -> None:
    """
    Append a list of features to the specified layer.

    Args:
        ago_item: The AGO item containing the layers.
        features_list: List of feature dictionaries to add.
        layer_num (int): The index of the layer to operate on (default 0).
    """
    if not features_list:
        _log.warning("No features to append")
        return

    layer = ago_item.layers[layer_num]
    layer.edit_features(adds=features_list)

    _log.info(f"Added {len(features_list)} features to {ago_item.itemid}")