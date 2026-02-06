import os
import math
import joblib
import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

MODEL_PATH = os.path.join(ROOT, "models", "points_model_v4.joblib")
FEATURE_CACHE_PATH = os.path.join(ROOT, "data", "points_feature_cache.csv")


def _normal_cdf(z: np.ndarray) -> np.ndarray:
    # Φ(z) = 0.5 * (1 + erf(z / sqrt(2)))
    return 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))


def _p_over_from_mu(mu: float, line: float, std: float) -> float:
    std = max(float(std), 1e-6)
    z = (float(line) - float(mu)) / std
    p = 1.0 - float(_normal_cdf(np.array([z]))[0])
    return float(np.clip(p, 0.001, 0.999))


class PointsPredictor:
    def __init__(self, artifact: dict, cache_df: pd.DataFrame):
        self.model = artifact["model"]
        self.features = list(artifact["features"])
        self.target_mode = artifact.get("target_mode", "direct")
        self.baseline_feature = artifact.get("baseline_feature", "pts_ma_10")
        self.delta_scale = float(artifact.get("delta_scale", 1.0))
        self.proj_blend_weight = float(artifact.get("proj_blend_weight", 0.0))
        self.baseline_floor_pct = float(artifact.get("baseline_floor_pct", 0.0))

        self.global_std = float(artifact.get("global_std", 6.5))
        self.player_std_map = artifact.get("player_std_map", {}) or {}

        self.calibrator = artifact.get("calibrator", None)

        self.p_clip_lo = float(artifact.get("p_clip_lo", 0.03))
        self.p_clip_hi = float(artifact.get("p_clip_hi", 0.97))
        self.std_multiplier = float(artifact.get("std_multiplier", 1.0))
        self.std_override = os.getenv("STD_MULTIPLIER_OVERRIDE", "").strip()

        self.cache_df = cache_df
        self._validate_cache()

    def _validate_cache(self):
        if self.cache_df is None or len(self.cache_df) == 0:
            raise RuntimeError(f"Feature cache empty or missing: {FEATURE_CACHE_PATH}")

        missing = [c for c in (["player_name"] + self.features) if c not in self.cache_df.columns]
        if missing:
            raise RuntimeError(
                f"Feature cache missing columns: {missing}. Have: {list(self.cache_df.columns)}"
            )

    def _row_for_player(self, player_name: str) -> pd.DataFrame:
        name = str(player_name).strip()

        # Some players can appear multiple times if cache kept multiple seasons
        # We'll take the most recent GAME_DATE if present, otherwise first row.
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

        # Ensure column order and numeric types
        X = row[self.features].copy()
        for c in self.features:
            X[c] = pd.to_numeric(X[c], errors="coerce")

        if X.isna().any(axis=None):
            # If any features are missing, we fail and caller can fallback
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

        # Predict expected points (mu)
        pred = float(self.model.predict(X)[0])
        if self.target_mode == "delta_from_pts_ma_10":
            baseline = float(X[self.baseline_feature].iloc[0])
            mu = (pred * self.delta_scale) + baseline
            if self.proj_blend_weight > 0 and "proj_pts_from_min" in X.columns:
                proj = float(X["proj_pts_from_min"].iloc[0])
                w = self.proj_blend_weight
                mu = (1 - w) * mu + w * proj
            if self.baseline_floor_pct > 0:
                mu = max(mu, self.baseline_floor_pct * baseline)
        else:
            mu = pred
        # Use player std if available, apply multiplier to reduce overconfidence
        std_mult = float(self.std_override) if self.std_override else self.std_multiplier
        std = self._std_for_player(player_name) * std_mult

        p_raw = _p_over_from_mu(mu, float(line), std)

        # Apply calibrator if present
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

        # Final clip guardrails
        p_final = float(np.clip(p_cal, self.p_clip_lo, self.p_clip_hi))
        return p_final

    def debug(self, player_name: str, line: float) -> dict:
        X = self._row_for_player(player_name)
        pred = float(self.model.predict(X)[0])
        baseline = None
        if self.target_mode == "delta_from_pts_ma_10":
            baseline = float(X[self.baseline_feature].iloc[0])
            mu = (pred * self.delta_scale) + baseline
            if self.proj_blend_weight > 0 and "proj_pts_from_min" in X.columns:
                proj = float(X["proj_pts_from_min"].iloc[0])
                w = self.proj_blend_weight
                mu = (1 - w) * mu + w * proj
            if self.baseline_floor_pct > 0:
                mu = max(mu, self.baseline_floor_pct * baseline)
        else:
            mu = pred
        std_base = self._std_for_player(player_name)
        std_mult = float(self.std_override) if self.std_override else self.std_multiplier
        std = std_base * std_mult
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
            "player": str(player_name),
            "line": float(line),
            "mu_pred_points": mu,
            "mu_raw_points": pred,
            "baseline_feature": self.baseline_feature if self.target_mode == "delta_from_pts_ma_10" else None,
            "baseline_value": baseline,
            "std_base": float(std_base),
            "std_multiplier": float(std_mult),
            "std_used": float(std),
            "p_raw": float(p_raw),
            "p_calibrated": float(p_cal),
            "p_final": float(p_final),
            "has_calibrator": bool(self.calibrator is not None),
            "clip": [float(self.p_clip_lo), float(self.p_clip_hi)],
            "features_used": self.features,
        }


_PREDICTOR = None


def get_predictor() -> PointsPredictor:
    global _PREDICTOR
    if _PREDICTOR is not None:
        return _PREDICTOR

    if not os.path.exists(MODEL_PATH):
        raise RuntimeError(f"Missing model artifact: {MODEL_PATH}. Run train_points_model.py first.")

    if not os.path.exists(FEATURE_CACHE_PATH):
        raise RuntimeError(
            f"Missing feature cache: {FEATURE_CACHE_PATH}. Run build_points_feature_cache.py first."
        )

    artifact = joblib.load(MODEL_PATH)

    cache_df = pd.read_csv(FEATURE_CACHE_PATH)

    _PREDICTOR = PointsPredictor(artifact, cache_df)
    return _PREDICTOR


def p_over_for_player_points(player_name: str, line: float) -> float:
    return get_predictor().p_over(player_name, float(line))
