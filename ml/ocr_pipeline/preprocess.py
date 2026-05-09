# ml/ocr_pipeline/preprocess.py
"""
Prétraitement image CNI avec OpenCV avant l'OCR Tesseract.
 
PIPELINE EN 5 ÉTAPES :
    1. Chargement depuis Azure ADLS (bytes → numpy array)
    2. Correction d'inclinaison (deskewing via transformée de Hough)
    3. Détection et découpe du document dans l'image
    4. Débruitage + amélioration contraste (CLAHE)
    5. Binarisation Otsu (noir/blanc — optimal pour Tesseract)
"""
import cv2
import numpy as np
def load_image_from_bytes(image_bytes: bytes) -> np.ndarray:
    """Convertit des bytes en tableau numpy BGR (format OpenCV)."""
    nparr = np.frombuffer(image_bytes, np.uint8)
    img   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("Image illisible — format non supporté ou fichier corrompu")
    return img
 
def deskew(image: np.ndarray) -> np.ndarray:
    """
    Corrige l'inclinaison (deskewing) via la transformée de Hough.
    L'angle médian des lignes dominantes est calculé puis compensé.
    Tolère les rotations jusqu'à ±45° (cas réels des photos mobiles).
    """
    gray  = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)
    lines = cv2.HoughLines(edges, 1, np.pi / 180, threshold=100)
    if lines is None:
        return image   # Pas de lignes détectées → image déjà droite
    angles = [line[0][1] * 180 / np.pi - 90 for line in lines]
    angle  = float(np.median(angles))
    if abs(angle) > 0.5:   # Correction seulement si angle significatif
        h, w = image.shape[:2]
        M    = cv2.getRotationMatrix2D((w // 2, h // 2), angle, 1.0)
        image = cv2.warpAffine(
            image, M, (w, h),
            flags=cv2.INTER_CUBIC,
            borderMode=cv2.BORDER_REPLICATE
        )
    return image
 
def detect_document_region(image: np.ndarray) -> np.ndarray:
    """
    Détecte et extrait la région du document CNI dans l'image.
    Cherche le plus grand quadrilatère (4 coins = carte d'identité).
    Retourne l'image complète si aucun document clair n'est détecté.
    """
    gray     = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blurred  = cv2.GaussianBlur(gray, (5, 5), 0)
    edged    = cv2.Canny(blurred, 30, 150)
    contours, _ = cv2.findContours(
        edged.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )
    contours = sorted(contours, key=cv2.contourArea, reverse=True)[:5]
    for cnt in contours:
        peri  = cv2.arcLength(cnt, True)
        approx = cv2.approxPolyDP(cnt, 0.02 * peri, True)
        if len(approx) == 4:   # Quadrilatère = document potentiel
            x, y, w, h = cv2.boundingRect(approx)
            if w * h > 0.1 * image.shape[0] * image.shape[1]:
                return image[y:y+h, x:x+w]   # Recadrage sur le document
    return image   # Fallback : image complète
 
def preprocess_for_ocr(image_bytes: bytes) -> np.ndarray:
    """
    Pipeline complet de prétraitement.
    Retourne l'image binarisée prête pour Tesseract.
    """
    img = load_image_from_bytes(image_bytes)
    img = deskew(img)
    img = detect_document_region(img)

    gray     = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    denoised = cv2.fastNlMeansDenoising(gray, h=10)
    # CLAHE is particularly effective on non-uniform mobile-camera lighting
    clahe    = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    enhanced = clahe.apply(denoised)

    _, binary = cv2.threshold(
        enhanced, 0, 255,
        cv2.THRESH_BINARY + cv2.THRESH_OTSU
    )
 
    # Upscaling si résolution insuffisante (Tesseract préfère ≥ 300 DPI)
    h, w = binary.shape
    if min(h, w) < 1000:
        scale  = 1000 / min(h, w)
        binary = cv2.resize(
            binary, None, fx=scale, fy=scale,
            interpolation=cv2.INTER_CUBIC
        )
 
    return binary
