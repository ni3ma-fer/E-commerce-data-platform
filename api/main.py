# api/main.py
"""
KiVendTout Data API — Exposition des données (couche finale du pipeline Medallion).

Endpoints opérationnels :
    POST /auth/token                  — Obtenir un JWT Bearer
    GET  /fraud-score/{txn_id}        — Score fraude pré-calculé ou XGBoost live
    POST /fraud-score/realtime        — Scoring temps réel avec features brutes
    POST /ocr/verify-id               — Pipeline OCR CNI -> décision KYC
    GET  /user/{id}/kyc-status        — Statut KYC d'un utilisateur
    DELETE /gdpr/erase/{id}           — Droit à l'effacement (RGPD Art. 17)

Documentation interactive : http://localhost:8000/docs
"""
import os
from datetime import timedelta

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from api.core.auth import authenticate_user, create_access_token
from api.routers   import fraud, gdpr, kyc, ocr

# ── Rate limiter (partage avec tous les routers) ──────────────────────────────
limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])

app = FastAPI(
    title        = "KiVendTout Data API",
    description  = __doc__,
    version      = "1.0.0",
    docs_url     = "/docs",
    redoc_url    = "/redoc",
    openapi_tags = [
        {"name": "Auth",            "description": "Authentification JWT"},
        {"name": "Fraud Detection", "description": "Scoring fraude XGBoost Champion"},
        {"name": "OCR / KYC",       "description": "Verification identite par OCR"},
        {"name": "KYC Status",      "description": "Statut KYC utilisateur"},
        {"name": "GDPR",            "description": "Droit a l'effacement (RGPD Art. 17)"},
        {"name": "Health",          "description": "Monitoring"},
    ],
)

# ── Middleware ────────────────────────────────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(fraud.router, tags=["Fraud Detection"])
app.include_router(ocr.router,   tags=["OCR / KYC"])
app.include_router(kyc.router,   tags=["KYC Status"])
app.include_router(gdpr.router,  tags=["GDPR"])


# ── Auth endpoint ─────────────────────────────────────────────────────────────
class TokenRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    expires_in:   int


@app.post(
    "/auth/token",
    response_model = TokenResponse,
    tags           = ["Auth"],
    summary        = "Obtenir un token JWT",
    description    = "POST username/password -> Bearer token valide 60 min.\n\nDemos: `admin/admin` ou `analyst/analyst`.",
)
@limiter.limit("10/minute")
async def login(body: TokenRequest, request: Request):
    user = authenticate_user(body.username, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Identifiants incorrects")
    ttl   = int(os.getenv("JWT_TOKEN_TTL_MINUTES", "60"))
    token = create_access_token(user["username"], user["role"], timedelta(minutes=ttl))
    return TokenResponse(access_token=token, expires_in=ttl * 60)


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"], summary="Etat de sante de l'API et des dependances")
async def health():
    from api.core.cache import cache_get, cache_set

    redis_ok = False
    try:
        cache_set("__health__", 1, ttl=5)
        redis_ok = cache_get("__health__") == 1
    except Exception:
        pass

    mlflow_ok = False
    try:
        import mlflow
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
        mlflow.tracking.MlflowClient().search_experiments()
        mlflow_ok = True
    except Exception:
        pass

    return {
        "status":  "ok",
        "redis":   "ok" if redis_ok   else "degraded",
        "mlflow":  "ok" if mlflow_ok  else "degraded",
        "version": app.version,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
