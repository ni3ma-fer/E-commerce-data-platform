# ml/ocr_pipeline/extract.py
"""
Extraction des données de la CNI via Tesseract OCR.
 
DONNÉES EXTRAITES :
    - Nom / Prénom (pour vérification cross-référence CRM)
    - Date de naissance → calcul de l'âge
    - Numéro de la carte (hashé SHA-256 immédiatement — RGPD)
 
CONFIGURATION TESSERACT :
    --oem 3   : LSTM + legacy OCR (meilleure précision sur docs structurés)
    --psm 3   : auto-segmentation de la page complète
    lang=fra+eng : dictionnaire français + anglais (CNI bilingue)
"""
import re, hashlib
import pytesseract
import numpy as np
from datetime import datetime
from typing import Optional
from dataclasses import dataclass
 
@dataclass
class CNIResult:
    """Résultat structuré de l'extraction OCR d'une CNI."""
    user_id:          str
    success:          bool
    raw_ocr_text:     str   = ""
    last_name:        Optional[str] = None
    first_name:       Optional[str] = None
    birth_date_str:   Optional[str] = None
    birth_date:       Optional[datetime] = None
    age:              Optional[int] = None
    is_minor:         bool  = False
    card_number_hash: Optional[str] = None   # SHA-256 — jamais le numéro brut
    confidence_score: float = 0.0             # Score moyen Tesseract (0-100)
    error_message:    Optional[str] = None
    # Décision métier : none / restrict_account / manual_review
    action_required:  str   = "none"
 
# ── Patterns regex pour CNI française ───────────────────────────────────
PATTERNS = {
    "nom":    r"(?:NOM|Nom)\s*[:;]?\s*([A-ZÉÈÊËÀÂÙÛÎ][A-ZÉÈÊËÀÂÙÛÎ\s-]+)",
    "prenom": r"(?:PRENOM|Prénom|PRÉNOM)\s*[:;]?\s*([A-ZÉÈÊËÀÂÙÛÎ][a-zA-Zéèê\s-]+)",
    "date":   r"(\d{2}[./\s]\d{2}[./\s]\d{4})",
    "numero": r"(?:N°|No|Numéro)?\s*([0-9A-Z]{12,15})",
}
 
def extract_cni_data(image: np.ndarray, user_id: str) -> CNIResult:
    """Extrait les données de la CNI depuis l'image pré-traitée."""
    result = CNIResult(user_id=user_id, success=False)
    try:
        # ── OCR Tesseract ─────────────────────────────────────────────────
        tess_config = "--oem 3 --psm 3"
        ocr_data = pytesseract.image_to_data(
            image, lang="fra+eng", config=tess_config,
            output_type=pytesseract.Output.DICT
        )
        confs, words = [], []
        for w, c in zip(ocr_data["text"], ocr_data["conf"]):
            ci = int(c)
            if ci != -1:
                confs.append(ci)
                if ci > 40:
                    words.append(w)
        result.confidence_score = float(np.mean(confs)) if confs else 0.0
        result.raw_ocr_text = " ".join(words)
 
        # ── Extraction par regex ──────────────────────────────────────────
        m_nom = re.search(PATTERNS["nom"], result.raw_ocr_text)
        if m_nom:
            result.last_name = m_nom.group(1).strip().upper()
 
        m_prenom = re.search(PATTERNS["prenom"], result.raw_ocr_text)
        if m_prenom:
            result.first_name = m_prenom.group(1).strip().capitalize()
 
        # ── Date de naissance → âge ───────────────────────────────────────
        m_date = re.search(PATTERNS["date"], result.raw_ocr_text)
        if m_date:
            date_str = m_date.group(1).replace(" ", ".").replace("/", ".")
            result.birth_date_str = date_str
            try:
                result.birth_date = datetime.strptime(date_str, "%d.%m.%Y")
                result.age      = (datetime.now() - result.birth_date).days // 365
                result.is_minor = result.age < 18
            except ValueError:
                result.error_message = f"Format date invalide : {date_str}"
 
        # ── Numéro CNI (hashé SHA-256 immédiatement — RGPD) ───────────────
        m_num = re.search(PATTERNS["numero"], result.raw_ocr_text)
        if m_num:
            raw_num = m_num.group(1)
            result.card_number_hash = (
                "SHA256_" + hashlib.sha256(raw_num.encode()).hexdigest()[:16]
            )
 
        # ── Décision métier ────────────────────────────────────────────────
        if result.is_minor:
            result.action_required = "restrict_account"
        elif result.confidence_score < 60 or result.birth_date is None:
            result.action_required = "manual_review"
        else:
            result.action_required = "none"
 
        result.success = result.birth_date is not None
 
    except Exception as e:
        result.error_message    = str(e)
        result.action_required  = "manual_review"
 
    return result
 
