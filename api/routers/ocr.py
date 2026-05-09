# api/routers/ocr.py
import io
import os
import sys
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from api.core.auth  import get_current_user
from api.core.cache import cache_set

router  = APIRouter()
limiter = Limiter(key_func=get_remote_address)

ALLOWED_TYPES = {"image/jpeg", "image/png", "image/webp"}
MAX_SIZE_MB   = 10


class OCRVerifyResponse(BaseModel):
    user_id:          str
    success:          bool
    last_name:        Optional[str]
    first_name:       Optional[str]
    age:              Optional[int]
    is_minor:         bool
    kyc_decision:     str       # "verified" | "restricted" | "manual_review" | "failed"
    confidence_score: float
    card_number_hash: Optional[str]
    error_message:    Optional[str]


@router.post(
    "/ocr/verify-id",
    response_model=OCRVerifyResponse,
    summary="Vérification d'identité par OCR (CNI)",
    description=(
        "Reçoit une image de CNI (JPEG/PNG), exécute le pipeline OCR Tesseract, "
        "retourne l'âge extrait et la décision KYC. "
        "Le numéro de carte est hashé SHA-256 immédiatement (RGPD Art. 5)."
    ),
)
@limiter.limit("20/minute")
async def verify_identity(
    request: Request,
    user_id: str,
    file:    UploadFile = File(..., description="Image de la CNI (JPEG ou PNG, max 10 Mo)"),
    user:    dict = Depends(get_current_user),
):
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(
            status_code=415,
            detail=f"Type de fichier non supporté. Acceptés : {', '.join(ALLOWED_TYPES)}",
        )

    image_bytes = await file.read()
    if len(image_bytes) > MAX_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"Image trop volumineuse (max {MAX_SIZE_MB} Mo)")

    try:
        from ml.ocr_pipeline.preprocess import preprocess_for_ocr
        from ml.ocr_pipeline.extract    import extract_cni_data
    except ImportError as exc:
        raise HTTPException(status_code=503, detail=f"Pipeline OCR indisponible : {exc}")

    try:
        processed = preprocess_for_ocr(image_bytes)
        result    = extract_cni_data(processed, user_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur OCR : {exc}")

    # Map action_required → kyc_decision vocabulary
    decision_map = {
        "none":             "verified",
        "restrict_account": "restricted",
        "manual_review":    "manual_review",
    }
    kyc_decision = decision_map.get(result.action_required, "failed") if result.success else "failed"

    # Cache the KYC result so the /kyc-status endpoint can return it immediately
    cache_set(f"kyc:{user_id}", {"status": kyc_decision, "age": result.age}, ttl=3600)

    return OCRVerifyResponse(
        user_id          = user_id,
        success          = result.success,
        last_name        = result.last_name,
        first_name       = result.first_name,
        age              = result.age,
        is_minor         = result.is_minor,
        kyc_decision     = kyc_decision,
        confidence_score = round(result.confidence_score, 2),
        card_number_hash = result.card_number_hash,
        error_message    = result.error_message,
    )
