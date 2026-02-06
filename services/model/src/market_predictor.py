from __future__ import annotations

import os
import math
import joblib
import numpy as np
import pandas as pd

from market_features import MARKET_MODEL_NAMES, MARKET_CACHE_NAMES

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _normal_cdf(z: np.ndarray) -> np.ndarray:
    return 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))


def _p_over_from_mu(mu: float, line: float, std: float) -> float:
    std = max(float(std), 1e-6)
    z = (float(line) - float(mu)) / std
    p = 1.0 - float(_normal_cdf(np.array([z]))[0])
    return float(np.clip(p, 0.001, 0.999))


class MarketPredictor:
    def __init__(self, market: str, artifact: dict, cache_df: pd.DataFrame):
        self.market = market
        self.model = artifact["model"]
        self.features = list(artifact["features"])

        self.global_std = float(artifact.get("global_std", 6.5))
        self.player_std_map = artifact.get("player_std_map", {}) or {}
        self.calibrator = artifact.get("calibrator", None)

        self.p_clip_lo = float(artifact.get("p_clip_lo", 0.03))
        self.p_clip_hi = float(artifact.get("p_clip_hi", 0.97))
        self.std_multiplier = float(artifact.get("std_multiplier", 1.0))

        self.cache_df = cache_df
        self._validate_cache()

    def _validate_cache(self):
        if self.cache_df is None or len(self.cache_df) == 0:
            raise RuntimeError(f"Feature cache empty for market={self.market}")

        missing = [c for c in (["player_name"] + self.features) if c not in self.cache_df.columns]
        if missing:
            raise RuntimeError(
                f"Feature cache missing columns for market={self.market}: {missing}"
            )

    def _row_for_player(self, player_name: str) -> pd.DataFrame:
        name = str(player_name).strip()
        sub = self.cache_df[self.cache_df["player_name"].astype(str) == name]
        if len(sub) == 0:
            raise RuntimeError(f"No feature cache row for player: {player_name}")

        if "GAME_DATE" in sub.columns:
            try:
                sub2 = sub.sort_values("GAME_DATE", ascending=False)
                row = sub2.iloc[[0]]
            except Exception:
                row = sub.iloc[[0]]
        else:
            row = sub.iloc[[0]]

        X = row[self.features].copy()
        for c in self.features:
            X[c] = pd.to_numeric(X[c], errors="coerce")
        if X.isna().any(axis=None):
            bad = X.columns[X.isna().any()].tolist()
            raise RuntimeError(f"Feature cache has NaNs for player={player_name} cols={bad}")

        return X.astype(float)

    def _std_for_player(self, player_name: str) -> float:
        s = self.player_std_map.get(str(player_name))
        if s is None:
            return self.global_std
        try:
            s = float(s)
            if not np.isfinite(s) or s <= 0:
                return self.global_std
            return s
        except Exception:
            return self.global_std

    def p_over(self, player_name: str, line: float) -> float:
        X = self._row_for_player(player_name)
        mu = float(self.model.predict(X)[0])
        std = self._std_for_player(player_name) * self.std_multiplier
        p_raw = _p_over_from_mu(mu, float(line), std)

        if self.calibrator is not None:
            try:
                if hasattr(self.calibrator, "predict_proba"):
                    p_cal = float(self.calibrator.predict_proba([[p_raw]])[0][1])
                else:
                    p_cal = float(self.calibrator.predict([p_raw])[0])
            except Exception:
                p_cal = p_raw
        else:
            p_cal = p_raw

        return float(np.clip(p_cal, self.p_clip_lo, self.p_clip_hi))

    def debug(self, player_name: str, line: float) -> dict:
        X = self._row_for_player(player_name)
        mu = float(self.model.predict(X)[0])
        std_base = self._std_for_player(player_name)
        std = std_base * self.std_multiplier
        p_raw = _p_over_from_mu(mu, float(line), std)

        if self.calibrator is not None:
            try:
                if hasattr(self.calibrator, "predict_proba"):
                    p_cal = float(self.calibrator.predict_proba([[p_raw]])[0][1])
                else:
                    p_cal = float(self.calibrator.predict([p_raw])[0])
            except Exception:
                p_cal = p_raw
        else:
            p_cal = p_raw

        p_final = float(np.clip(p_cal, self.p_clip_lo, self.p_clip_hi))
        return {
            "market": self.market,
            "player": str(player_name),
            "line": float(line),
            "mu_pred": mu,
            "std_base": float(std_base),
            "std_multiplier": float(self.std_multiplier),
            "std_used": float(std),
            "p_raw": float(p_raw),
            "p_calibrated": float(p_cal),
            "p_final": float(p_final),
            "has_calibrator": bool(self.calibrator is not None),
            "clip": [float(self.p_clip_lo), float(self.p_clip_hi)],
            "features_used": self.features,
        }


_MARKET_PREDICTORS: dict[str, MarketPredictor] = {}


def _model_path_for_market(market: str) -> str:
    name = MARKET_MODEL_NAMES.get(market)
    if not name:
        raise RuntimeError(f"No model mapping for market={market}")
    return os.path.join(ROOT, "models", name)


def _cache_path_for_market(market: str) -> str:
    name = MARKET_CACHE_NAMES.get(market)
    if not name:
        raise RuntimeError(f"No cache mapping for market={market}")
    return os.path.join(ROOT, "data", name)


def can_predict_market(market: str) -> bool:
    try:
        return os.path.exists(_model_path_for_market(market)) and os.path.exists(_cache_path_for_market(market))
    except Exception:
        return False


def get_market_predictor(market: str) -> MarketPredictor:
    if market in _MARKET_PREDICTORS:
        return _MARKET_PREDICTORS[market]

    model_path = _model_path_for_market(market)
    cache_path = _cache_path_for_market(market)

    if not os.path.exists(model_path):
        raise RuntimeError(f"Missing model artifact: {model_path}")
    if not os.path.exists(cache_path):
        raise RuntimeError(f"Missing feature cache: {cache_path}")

    artifact = joblib.load(model_path)
    cache_df = pd.read_csv(cache_path)
    pred = MarketPredictor(market, artifact, cache_df)
    _MARKET_PREDICTORS[market] = pred
    return pred


def p_over_for_market(player_name: str, line: float, market: str) -> float:
    return get_market_predictor(market).p_over(player_name, float(line))
