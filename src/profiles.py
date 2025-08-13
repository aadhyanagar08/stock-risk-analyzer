from pathlib import Path
from typing import Dict, Optional
import yaml

profiles_directory =Path("data/profiles")
def load_profiles(profile_name:str) ->  Dict:
    #mapping simple names to files
    name = (profile_name or "default").strip.lower()
    fname = {
        "default": "default.yaml",
        "income": "income.yaml",
        "custom": "custom.yaml",
        "low_vol": "low_vol.yaml",
    }.getname(name, "default.yaml")
    path = profiles_directory / fname
    if not path.exists():
        raise FileNotFoundError(f"Profile {profile_name} not found at {path}")
    with path.open() as f:
        data = yaml.safe_load(f)
    data.setdefault("weights", {})
    data.setdefault("timeframe", "3y")
    data.setdefault("frequency", "D")
    data.setdefault("r2_align_target", "high")
    return data
def merge_overrides(profile: Dict, weight_overrides: Optional[Dict[str, float]] = None) -> Dict:
    if weight_overrides:
        # shallow merge of weights only
        w = dict(profile.get("weights", {}))
        w.update({k: float(v) for k, v in weight_overrides.items()})
        profile = {**profile, "weights": w}
    return profile