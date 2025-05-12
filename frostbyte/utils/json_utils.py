"""
JSON serialization utilities for Frostbyte.
"""
import json
import numpy as np
import pandas as pd
from typing import Any


class FrostbyteJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for Frostbyte that handles numpy types."""
    
    def default(self, obj: Any) -> Any:
        """Convert numpy types to standard Python types for JSON serialization."""
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.bool_):
            return bool(obj)
        if pd.isna(obj):
            return None
        return super().default(obj)


def json_dumps(obj: Any) -> str:
    """Wrapper for json.dumps that uses FrostbyteJSONEncoder."""
    return json.dumps(obj, cls=FrostbyteJSONEncoder)


def json_loads(json_str: str) -> Any:
    """Wrapper for json.loads."""
    return json.loads(json_str)
