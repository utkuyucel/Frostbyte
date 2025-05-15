import json
from typing import Any

import numpy as np
import pandas as pd


class FrostbyteJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
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
    return json.dumps(obj, cls=FrostbyteJSONEncoder)


def json_loads(json_str: str) -> Any:
    return json.loads(json_str)
