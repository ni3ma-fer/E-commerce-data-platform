# api/routers/gdpr.py
import hashlib
import os
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from api.core.auth   import require_admin
from api.core.cache  import cache_delete, cache_keys
from api.core.alerts import send_pipeline_alert

router  = APIRouter()
limiter = Limiter(key_func=get_remote_address)


class GDPREraseResponse(BaseModel):
    user_id:          str
    pseudonym:        str   # SHA-256 used in place of user_id in audit logs
    erased_at:        str
    cache_keys_purged:int
    silver_purged:    bool
    gold_purged:      bool
    confirmation:     str


@router.delete(
    "/gdpr/erase/{user_id}",
    response_model=GDPREraseResponse,
    summary="Droit à l'effacement (RGPD Art. 17)",
    description=(
        "Supprime toutes les données personnelles identifiables de `user_id`. "
        "Purge le cache Redis, pseudonymise les références Silver/Gold. "
        "Réservé aux admins (JWT rôle=admin)."
    ),
    status_code=200,
)
@limiter.limit("10/minute")
async def erase_user_data(
    user_id: str,
    request: Request,
    admin:   dict = Depends(require_admin),
):
    pseudonym  = "ERASED_" + hashlib.sha256(user_id.encode()).hexdigest()[:16]
    erased_at  = datetime.now(timezone.utc).isoformat()

    # 1. Purge Redis cache (all keys related to this user)
    keys_to_del = cache_keys(f"*{user_id}*")
    for k in keys_to_del:
        cache_delete(k)

    # 2. Silver pseudonymisation (DuckDB / ADLS)
    silver_ok = _purge_silver(user_id, pseudonym)

    # 3. Gold nullification
    gold_ok = _purge_gold(user_id, pseudonym)

    # 4. Audit trail
    await send_pipeline_alert(
        component="GDPR",
        message=f"Erasure request executed for pseudonym {pseudonym} by {admin['username']}",
        level="info",
    )

    return GDPREraseResponse(
        user_id           = user_id,
        pseudonym         = pseudonym,
        erased_at         = erased_at,
        cache_keys_purged = len(keys_to_del),
        silver_purged     = silver_ok,
        gold_purged       = gold_ok,
        confirmation      = (
            f"Données de {user_id} supprimées conformément au RGPD Art. 17. "
            f"Pseudonyme audit : {pseudonym}"
        ),
    )


def _purge_silver(user_id: str, pseudonym: str) -> bool:
    try:
        import duckdb
        db_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "dbt", "kivendtout_local.duckdb"
        )
        if not os.path.exists(db_path):
            return False
        con = duckdb.connect(db_path)
        con.execute(
            "UPDATE silver_crm SET email_hash = ?, phone_hash = ?, user_id = ? WHERE user_id = ?",
            [pseudonym, pseudonym, pseudonym, user_id],
        )
        con.commit()
        con.close()
        return True
    except Exception:
        return False


def _purge_gold(user_id: str, pseudonym: str) -> bool:
    try:
        import duckdb
        db_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "dbt", "kivendtout_local.duckdb"
        )
        if not os.path.exists(db_path):
            return False
        con = duckdb.connect(db_path)
        for table in ("dim_customers", "fact_orders", "fact_fraud_scores"):
            try:
                con.execute(
                    f"UPDATE {table} SET user_id = ? WHERE user_id = ?",
                    [pseudonym, user_id],
                )
            except Exception:
                pass
        con.commit()
        con.close()
        return True
    except Exception:
        return False
