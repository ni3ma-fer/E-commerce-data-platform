# api/routers/fraud.py
import os
import sys
import hashlib
from typing import Optional

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from api.core.auth   import get_current_user
from api.core.cache  import cache_get, cache_set
from api.core.alerts import send_fraud_alert

router  = APIRouter()
limiter = Limiter(key_func=get_remote_address)

# ── Label mappings (mirror sklearn LabelEncoder alphabetical sort on training data) ──
# These match the encodings produced by prepare_features() in train.py on synthetic data.
_KYC_STATUS    = {"pending": 0, "rejected": 1, "unknown": 2, "verified": 3}
_AGE_GROUP     = {"18-25": 0, "26-35": 1, "36-50": 2, "51-65": 3, "65+": 4}
_RISK_CATEGORY = {"high": 0, "low": 1, "medium": 2}
_PAYMENT_METHOD = {"bank_transfer": 0, "card": 1, "crypto": 2, "wallet": 3}
_COUNTRY_ISO   = {"CN": 0, "DE": 1, "FR": 2, "GB": 3, "NG": 4, "RU": 5, "US": 6}


def _encode_cat(value: str, mapping: dict) -> int:
    return mapping.get(str(value).lower() if value else "unknown",
                       mapping.get("unknown", 0))


# ── Lazy model loader ─────────────────────────────────────────────────────────
_model = None

def _get_model():
    global _model
    if _model is None:
        import mlflow.xgboost
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
        try:
            _model = mlflow.xgboost.load_model("models:/fraud-xgboost-champion/Production")
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Modèle MLflow indisponible : {exc}")
    return _model


# ── Schémas ───────────────────────────────────────────────────────────────────
class FraudScoreResponse(BaseModel):
    model_config = {"protected_namespaces": ()}

    transaction_id:    str
    fraud_probability: float
    fraud_decision:    str
    model_version:     str
    cached:            bool


class ScoreRequest(BaseModel):
    """Features explicites pour un scoring temps réel (20 features = schéma champion)."""
    # Numeric (7)
    txn_count_5min:      Optional[float] = 1.0
    txn_count_1h:        Optional[float] = 2.0
    txn_count_30d:       Optional[float] = 30.0
    avg_amount_30d:      Optional[float] = 120.0
    amount_vs_avg_ratio: Optional[float] = 1.2
    amount_stddev_30d:   Optional[float] = 40.0
    geo_distance_km:     Optional[float] = 5.0
    # Categorical — accepted as strings, encoded internally (7)
    kyc_status:           Optional[str]  = "verified"
    age_group:            Optional[str]  = "36-50"
    risk_category:        Optional[str]  = "low"
    category_id:          Optional[str]  = "10"
    payment_method:       Optional[str]  = "card"
    billing_country_iso:  Optional[str]  = "FR"
    shipping_country_iso: Optional[str]  = "FR"
    # Binary (6)
    is_cross_border:      Optional[int]  = 0
    user_is_minor:        Optional[int]  = 0
    is_first_transaction: Optional[int]  = 0
    is_new_card:          Optional[int]  = 0
    is_new_merchant:      Optional[int]  = 0
    ip_geo_mismatch:      Optional[int]  = 0


def _decision(score: float) -> str:
    if score >= 0.80:
        return "BLOCK"
    if score >= 0.50:
        return "3DS_REQUIRED"
    return "APPROVED"


def _encode_request(req: ScoreRequest) -> np.ndarray:
    """Convert ScoreRequest → (1, 20) float array matching champion model input order."""
    # category_id is numeric-as-string ("1"–"49"); LabelEncoder sorts lexicographically.
    # Map to its alphabetical rank among training values (safe fallback: 0).
    cat_ids = [str(i) for i in range(1, 50)]
    cat_id_map = {v: i for i, v in enumerate(sorted(cat_ids))}

    return np.array([[
        req.txn_count_5min,
        req.txn_count_1h,
        req.txn_count_30d,
        req.avg_amount_30d,
        req.amount_vs_avg_ratio,
        req.amount_stddev_30d,
        req.geo_distance_km,
        _KYC_STATUS.get(req.kyc_status, 2),
        _AGE_GROUP.get(req.age_group, 2),
        _RISK_CATEGORY.get(req.risk_category, 1),
        cat_id_map.get(str(req.category_id), 0),
        _PAYMENT_METHOD.get(req.payment_method, 1),
        _COUNTRY_ISO.get(req.billing_country_iso, 2),
        _COUNTRY_ISO.get(req.shipping_country_iso, 2),
        req.is_cross_border,
        req.user_is_minor,
        req.is_first_transaction,
        req.is_new_card,
        req.is_new_merchant,
        req.ip_geo_mismatch,
    ]], dtype=float)


def _synthetic_features(txn_id: str) -> np.ndarray:
    """
    Derive pseudo-random deterministic 20-feature row from txn_id for GET /fraud-score.
    In production this would read the pre-computed score from gold/fraud_scores Delta.
    """
    seed = int(hashlib.sha256(txn_id.encode()).hexdigest()[:8], 16)
    rng  = np.random.default_rng(seed)
    cat_ids = [str(i) for i in range(1, 50)]
    cat_id_map = {v: i for i, v in enumerate(sorted(cat_ids))}
    return np.array([[
        rng.uniform(1, 5),          # txn_count_5min
        rng.uniform(1, 10),         # txn_count_1h
        rng.uniform(5, 100),        # txn_count_30d
        rng.uniform(20, 300),       # avg_amount_30d
        rng.uniform(0.5, 3),        # amount_vs_avg_ratio
        rng.uniform(10, 150),       # amount_stddev_30d
        rng.uniform(0, 500),        # geo_distance_km
        int(rng.integers(0, 4)),    # kyc_status (0–3)
        int(rng.integers(0, 5)),    # age_group (0–4)
        int(rng.integers(0, 3)),    # risk_category (0–2)
        int(rng.integers(0, 49)),   # category_id (0–48)
        int(rng.integers(0, 4)),    # payment_method (0–3)
        int(rng.integers(0, 7)),    # billing_country_iso (0–6)
        int(rng.integers(0, 7)),    # shipping_country_iso (0–6)
        int(rng.random() < 0.1),   # is_cross_border
        0,                          # user_is_minor
        int(rng.random() < 0.05),  # is_first_transaction
        int(rng.random() < 0.05),  # is_new_card
        int(rng.random() < 0.05),  # is_new_merchant
        int(rng.random() < 0.05),  # ip_geo_mismatch
    ]], dtype=float)


# ── Endpoints ─────────────────────────────────────────────────────────────────
@router.get(
    "/fraud-score/{txn_id}",
    response_model=FraudScoreResponse,
    summary="Score fraude pour une transaction",
    description=(
        "Retourne la probabilité de fraude pour `txn_id`. "
        "Lecture depuis le cache Redis, puis scoring XGBoost Champion si absent."
    ),
)
@limiter.limit("100/minute")
async def get_fraud_score(
    txn_id:  str,
    request: Request,
    user:    dict = Depends(get_current_user),
):
    cache_key = f"fraud:{txn_id}"
    cached    = cache_get(cache_key)
    if cached:
        return FraudScoreResponse(**cached, cached=True)

    model    = _get_model()
    feats    = _synthetic_features(txn_id)
    score    = float(model.predict_proba(feats)[0, 1])
    decision = _decision(score)

    payload = {
        "transaction_id":    txn_id,
        "fraud_probability": round(score, 6),
        "fraud_decision":    decision,
        "model_version":     "fraud-xgboost-champion/Production",
    }
    cache_set(cache_key, payload, ttl=300)
    await send_fraud_alert(txn_id, user["username"], score, decision)
    return FraudScoreResponse(**payload, cached=False)


@router.post(
    "/fraud-score/realtime",
    response_model=FraudScoreResponse,
    summary="Scoring temps réel avec features explicites",
    description=(
        "Score une transaction à partir de ses 20 features brutes (bypass cache). "
        "Les catégorielles (kyc_status, age_group…) sont acceptées en string."
    ),
)
@limiter.limit("60/minute")
async def score_realtime(
    features: ScoreRequest,
    request:  Request,
    user:     dict = Depends(get_current_user),
):
    model    = _get_model()
    X        = _encode_request(features)
    score    = float(model.predict_proba(X)[0, 1])
    decision = _decision(score)
    txn_id   = "realtime-" + hashlib.sha256(str(features.model_dump()).encode()).hexdigest()[:8]

    await send_fraud_alert(txn_id, user["username"], score, decision)
    return FraudScoreResponse(
        transaction_id=txn_id,
        fraud_probability=round(score, 6),
        fraud_decision=decision,
        model_version="fraud-xgboost-champion/Production",
        cached=False,
    )
