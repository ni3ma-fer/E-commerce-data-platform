# api/routers/kyc.py
import os
import sys
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from api.core.auth  import get_current_user
from api.core.cache import cache_get, cache_set

router  = APIRouter()
limiter = Limiter(key_func=get_remote_address)


class KYCStatusResponse(BaseModel):
    user_id:           str
    kyc_status:        str           # verified | pending | rejected | none
    age_group:         Optional[str]
    is_minor:          bool
    account_restricted:bool
    last_verified_at:  Optional[str]
    source:            str           # "cache" | "silver_crm" | "mock"


def _lookup_silver_crm(user_id: str) -> Optional[dict]:
    """Query Silver CRM in DuckDB when available."""
    try:
        import duckdb
        db_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "dbt", "kivendtout_local.duckdb"
        )
        if not os.path.exists(db_path):
            return None
        con = duckdb.connect(db_path, read_only=True)
        row = con.execute(
            "SELECT id_verification_status, age_group FROM silver_crm WHERE user_id = ? LIMIT 1",
            [user_id],
        ).fetchone()
        con.close()
        if row:
            return {"kyc_status": row[0] or "pending", "age_group": row[1]}
    except Exception:
        pass
    return None


def _mock_status(user_id: str) -> dict:
    """Deterministic mock KYC data based on user_id hash for demo endpoints."""
    import hashlib
    h = int(hashlib.sha256(user_id.encode()).hexdigest()[:4], 16)
    statuses   = ["verified", "verified", "verified", "pending", "rejected", "none"]
    age_groups = ["26-35", "36-50", "18-25", "51-65", "26-35", "18-25"]
    return {
        "kyc_status": statuses[h % len(statuses)],
        "age_group":  age_groups[h % len(age_groups)],
    }


@router.get(
    "/user/{user_id}/kyc-status",
    response_model=KYCStatusResponse,
    summary="Statut KYC d'un utilisateur",
    description=(
        "Retourne le statut de vérification d'identité. "
        "Ordre de lecture : cache Redis → Silver CRM (DuckDB) → mock."
    ),
)
@limiter.limit("200/minute")
async def get_kyc_status(
    user_id: str,
    request: Request,
    user:    dict = Depends(get_current_user),
):
    cache_key = f"kyc:{user_id}"
    cached    = cache_get(cache_key)
    if cached:
        status = cached.get("status", "pending")
        return KYCStatusResponse(
            user_id            = user_id,
            kyc_status         = status,
            age_group          = cached.get("age_group"),
            is_minor           = status == "restricted",
            account_restricted = status in ("restricted", "rejected"),
            last_verified_at   = cached.get("verified_at"),
            source             = "cache",
        )

    silver = _lookup_silver_crm(user_id)
    if silver:
        cache_set(cache_key, silver, ttl=600)
        return KYCStatusResponse(
            user_id            = user_id,
            kyc_status         = silver["kyc_status"],
            age_group          = silver.get("age_group"),
            is_minor           = False,
            account_restricted = silver["kyc_status"] in ("rejected",),
            last_verified_at   = None,
            source             = "silver_crm",
        )

    mock = _mock_status(user_id)
    return KYCStatusResponse(
        user_id            = user_id,
        kyc_status         = mock["kyc_status"],
        age_group          = mock["age_group"],
        is_minor           = False,
        account_restricted = mock["kyc_status"] == "rejected",
        last_verified_at   = None,
        source             = "mock",
    )
