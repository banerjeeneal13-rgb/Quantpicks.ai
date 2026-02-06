from __future__ import annotations

import joblib
import pandas as pd
from math import erf, sqrt
from pathlib import Path
from unidecode import unidecode
import re

_MODEL = None
_PLAYER_STD = None

def _norm_name(s: str) -> str:
    s = unidecode((s or "").lower().strip())
    s = s.replace(".", "")
    s = re.sub(r"[^a-z\s']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b$", "", s).strip()
    return s

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))

def load_points_artifact():
    global _MODEL
    if _MODEL is None:
        model_path = Path(__file__).resolve().parents[1] / "models" / "points_model.joblib"
        _MODEL = joblib.load(model_path)
    return _MODEL

def load_player_std():
    global _PLAYER_STD
    if _PLAYER_STD is None:
        p = Path(__file__).resolve().parents[1] / "data" / "player_points_std.csv"
        if p.exists():
            df = pd.read_csv(p)
            df["key"] = df["player_name"].apply(_norm_name)
            _PLAYER_STD = dict(zip(df["key"], df["player_std"].astype(float)))
        else:
            _PLAYER_STD = {}
    return _PLAYER_STD

def p_over_points(features: dict, line: float, player_name: str | None = None) -> float:
    art = load_points_artifact()
    model = art["model"]

    # Mean prediction (with correct feature names)
    X = pd.DataFrame([[features[f] for f in art["features"]]], columns=art["features"])
    mean = float(model.predict(X)[0])

    # Player-specific std fallback to global
    std_map = load_player_std()
    std = float(art["global_std"])
    if player_name:
        std = float(std_map.get(_norm_name(player_name), std))

    # Safety clamps (prevents insane certainty)
    std = max(std, 3.0)
    std = min(std, 18.0)

    z = ((float(line) + 0.5) - mean) / std
    p_under = _norm_cdf(z)
    p_over = 1.0 - p_under

    # clamp probabilities slightly away from 0/1
    return float(min(max(p_over, 0.01), 0.99))
