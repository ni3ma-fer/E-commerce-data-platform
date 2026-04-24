"""
=============================================================================
shared_data_pool.py  —  KiVendTout Data Platform
=============================================================================
Module partagé entre les 4 scripts de génération de données.

POURQUOI CE MODULE EXISTE :
  La cohérence relationnelle est CRITIQUE pour les cas d'usage ML et fraude.
  Un utilisateur qui achète dans payment_producer.py doit exister dans le CRM
  (dag_bronze_crm.py), avoir un historique clickstream cohérent, et une commande
  logistique associée. Sans pool commun, les données sont inutilisables pour les
  analyses croisées et le feature engineering du modèle XGBoost.

CAS D'USAGE DIRECTION ADRESSÉS :
  - Détection fraude : cohérence user_id / IP / device entre tous les flux
  - ML KYC : les utilisateurs mineurs sont tracés dans TOUS les systèmes
  - Analyse comportementale : même user_id du clic jusqu'à la livraison
=============================================================================
"""

import random
import hashlib
from datetime import datetime, date
from typing import Optional

# ─── GRAINE FIXE POUR REPRODUCTIBILITÉ ────────────────────────────────────────
# La même graine garantit que les pools sont IDENTIQUES à chaque redémarrage
# des scripts. Essentiel pour la cohérence relationnelle inter-scripts.
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# =============================================================================
# POOL UTILISATEURS  (USR-000001 → USR-005000)
# =============================================================================

TOTAL_USERS = 5000

# Segments clients (reflète la réalité e-commerce : majorité Silver)
USER_SEGMENTS = {
    "bronze": 0.45,   # 2250 users — acheteurs occasionnels
    "silver": 0.30,   # 1500 users — acheteurs réguliers
    "gold":   0.18,   # 900  users — acheteurs fréquents
    "vip":    0.07,   # 350  users — top clients
}

# Distribution âges : inclut INTENTIONNELLEMENT des mineurs (5%)
# pour tester le pipeline de vérification KYC et les blocages fraude
AGE_DISTRIBUTION = {
    "minor_14_17":  0.05,   # 250 users — CRITIQUES pour test KYC
    "young_18_25":  0.22,   # 1100 users — fort taux d'achat mobile
    "adult_26_35":  0.28,   # 1400 users — segment le plus actif
    "adult_36_45":  0.20,   # 1000 users — pouvoir d'achat élevé
    "adult_46_55":  0.15,   # 750  users
    "senior_56_75": 0.10,   # 500  users
}

# Statuts de vérification KYC (utilisé dans CRM + payment_producer)
KYC_STATUS_DISTRIBUTION = {
    "verified":  0.55,   # 2750 users — vérifiés, accès complet
    "pending":   0.15,   # 750  users — en cours de vérification
    "rejected":  0.08,   # 400  users — rejetés (faux docs, mineur détecté)
    "none":      0.22,   # 1100 users — jamais soumis
}

# Pays de résidence (utilisé pour détecter les incohérences géographiques)
COUNTRY_DISTRIBUTION = {
    "FR": 0.65,
    "BE": 0.08,
    "CH": 0.07,
    "DE": 0.05,
    "IT": 0.04,
    "ES": 0.04,
    "NL": 0.03,
    "LU": 0.02,
    "PT": 0.02,
}

def _weighted_choice(distribution: dict) -> str:
    keys = list(distribution.keys())
    weights = list(distribution.values())
    return random.choices(keys, weights=weights, k=1)[0]

def _birth_date_for_age_group(group: str) -> date:
    today = date.today()
    ranges = {
        "minor_14_17":  (14, 17),
        "young_18_25":  (18, 25),
        "adult_26_35":  (26, 35),
        "adult_36_45":  (36, 45),
        "adult_46_55":  (46, 55),
        "senior_56_75": (56, 75),
    }
    min_age, max_age = ranges[group]
    age = random.randint(min_age, max_age)
    # Décaler de quelques jours pour éviter d'être exactement à la limite
    birth_year = today.year - age
    try:
        bd = date(birth_year, random.randint(1, 12), random.randint(1, 28))
    except ValueError:
        bd = date(birth_year, 1, 1)
    return bd

# Génération déterministe du pool complet au chargement du module
def build_user_pool() -> dict:
    """
    Construit le dictionnaire complet de tous les utilisateurs.
    DÉTERMINISTE : la même graine = le même pool à chaque exécution.
    
    Structure retournée par user_id :
      age_group, birth_date, age, kyc_status, country, segment,
      id_document_path (simulé), email_hash
    """
    pool = {}
    rng = random.Random(RANDOM_SEED)

    age_groups = list(AGE_DISTRIBUTION.keys())
    age_weights = list(AGE_DISTRIBUTION.values())
    kyc_groups  = list(KYC_STATUS_DISTRIBUTION.keys())
    kyc_weights = list(KYC_STATUS_DISTRIBUTION.values())
    country_keys   = list(COUNTRY_DISTRIBUTION.keys())
    country_weights = list(COUNTRY_DISTRIBUTION.values())
    segment_keys    = list(USER_SEGMENTS.keys())
    segment_weights = list(USER_SEGMENTS.values())

    for i in range(1, TOTAL_USERS + 1):
        uid = f"USR-{i:06d}"
        age_group = rng.choices(age_groups, weights=age_weights, k=1)[0]
        birth_date = _birth_date_for_age_group_rng(age_group, rng)
        today = date.today()
        age = today.year - birth_date.year - (
            (today.month, today.day) < (birth_date.month, birth_date.day)
        )
        is_minor = age < 18

        # Les mineurs ont des statuts KYC spécifiques :
        # - Si vérifiés → ils sont "rejected" (processus KYC les a détectés)
        # - Sinon → pending ou none (pas encore passé par KYC)
        if is_minor:
            kyc_status = rng.choices(
                ["rejected", "pending", "none"], weights=[0.4, 0.3, 0.3], k=1
            )[0]
        else:
            kyc_status = rng.choices(kyc_groups, weights=kyc_weights, k=1)[0]

        country = rng.choices(country_keys, weights=country_weights, k=1)[0]
        segment = rng.choices(segment_keys, weights=segment_weights, k=1)[0]

        # Chemin simulé vers l'image de la carte d'identité (pour le modèle OCR)
        # En prod : s3://kivendtout-raw/id_cards/{uid}.jpg après upload
        if kyc_status in ("verified", "rejected"):
            doc_path = f"s3://kivendtout-bronze/id_cards/{uid}_cni.jpg"
        elif kyc_status == "pending":
            doc_path = f"s3://kivendtout-bronze/id_cards/{uid}_cni_pending.jpg"
        else:
            doc_path = None  # Pas encore soumis

        pool[uid] = {
            "user_id":              uid,
            "age_group":            age_group,
            "birth_date":           birth_date,
            "age":                  age,
            "is_minor":             is_minor,
            "kyc_status":           kyc_status,
            "country":              country,
            "segment":              segment,
            "id_document_path":     doc_path,
            # Hash email stable pour cohérence inter-scripts (jamais l'email réel)
            "email_domain":         rng.choice([
                "gmail.com", "outlook.com", "yahoo.fr", "orange.fr",
                "laposte.net", "free.fr", "sfr.fr", "hotmail.fr"
            ]),
            "registration_date":    date(
                rng.randint(2019, 2024),
                rng.randint(1, 12),
                rng.randint(1, 28)
            ),
        }
    return pool

def _birth_date_for_age_group_rng(group: str, rng: random.Random) -> date:
    today = date.today()
    ranges = {
        "minor_14_17":  (14, 17),
        "young_18_25":  (18, 25),
        "adult_26_35":  (26, 35),
        "adult_36_45":  (36, 45),
        "adult_46_55":  (46, 55),
        "senior_56_75": (56, 75),
    }
    min_age, max_age = ranges[group]
    age = rng.randint(min_age, max_age)
    birth_year = today.year - age
    month = rng.randint(1, 12)
    day = rng.randint(1, 28)
    try:
        return date(birth_year, month, day)
    except ValueError:
        return date(birth_year, 1, 1)

# Pool global — chargé une seule fois au démarrage
USER_POOL: dict = build_user_pool()
USER_IDS:  list = list(USER_POOL.keys())

# Sous-listes précalculées (optimisation : évite de filtrer à chaque événement)
MINOR_USER_IDS    = [uid for uid, u in USER_POOL.items() if u["is_minor"]]
ADULT_USER_IDS    = [uid for uid, u in USER_POOL.items() if not u["is_minor"]]
VERIFIED_USER_IDS = [uid for uid, u in USER_POOL.items() if u["kyc_status"] == "verified"]
UNVERIFIED_IDS    = [uid for uid, u in USER_POOL.items()
                     if u["kyc_status"] in ("pending", "none", "rejected")]

# =============================================================================
# CATALOGUE PRODUITS
# =============================================================================
# Chaque catégorie a : id, nom, price_range, adult_only, typical_margin

PRODUCT_CATALOG = {
    # ── Catégories STANDARD ───────────────────────────────────────────────────
    "CAT-ELEC": {
        "name": "Électronique",
        "products": [
            {"id": f"PROD-ELEC-{i:04d}", "name": n, "price_range": pr, "adult_only": False}
            for i, (n, pr) in enumerate([
                ("Smartphone Samsung Galaxy A55", (329.99, 449.99)),
                ("Écouteurs Sony WH-1000XM5",    (249.99, 319.99)),
                ("Tablette iPad 10e génération",  (449.99, 599.99)),
                ("Clavier mécanique Logitech G",  (79.99,  139.99)),
                ("Webcam Logitech C920",           (59.99,   89.99)),
                ("Disque SSD Samsung 1TB",         (69.99,   99.99)),
                ("Montre connectée Garmin Venu",   (199.99, 299.99)),
                ("Enceinte Bluetooth JBL Charge",  (89.99,  139.99)),
                ("Chargeur USB-C 65W Anker",       (24.99,   39.99)),
                ("Câble HDMI 2.1 4K 2m",           (12.99,   24.99)),
            ], start=1)
        ],
        "adult_only": False,
    },
    "CAT-MODE": {
        "name": "Mode & Vêtements",
        "products": [
            {"id": f"PROD-MODE-{i:04d}", "name": n, "price_range": pr, "adult_only": False}
            for i, (n, pr) in enumerate([
                ("Sneakers Nike Air Max 270",     (89.99,  149.99)),
                ("Jean slim Levi's 512",           (59.99,   99.99)),
                ("Veste en cuir femme Zara",       (79.99,  139.99)),
                ("T-shirt oversize H&M",           (14.99,   29.99)),
                ("Robe de soirée Mango",           (49.99,   89.99)),
                ("Manteau laine & cashmere",      (129.99, 249.99)),
                ("Chaussures de running Adidas",   (79.99,  129.99)),
                ("Sac à main cuir Longchamp",     (149.99, 299.99)),
                ("Lunettes de soleil Ray-Ban",     (99.99,  179.99)),
                ("Montre femme Fossil",            (89.99,  169.99)),
            ], start=1)
        ],
        "adult_only": False,
    },
    "CAT-MAISON": {
        "name": "Maison & Décoration",
        "products": [
            {"id": f"PROD-MAIS-{i:04d}", "name": n, "price_range": pr, "adult_only": False}
            for i, (n, pr) in enumerate([
                ("Aspirateur robot iRobot Roomba", (249.99, 399.99)),
                ("Cafetière Nespresso Vertuo",      (89.99,  149.99)),
                ("Coussin velours 45x45cm",          (12.99,   24.99)),
                ("Lampe de bureau LED tactile",      (29.99,   59.99)),
                ("Cadre photo personnalisé A4",      (19.99,   39.99)),
                ("Plaid polaire 180x200cm",           (24.99,   49.99)),
                ("Robot cuiseur Thermomix TM6",     (899.99, 1299.99)),
                ("Bougie parfumée Diptyque",          (49.99,   79.99)),
                ("Diffuseur huiles essentielles",     (19.99,   39.99)),
                ("Organisation bureau bambou",        (29.99,   59.99)),
            ], start=1)
        ],
        "adult_only": False,
    },
    "CAT-SPORT": {
        "name": "Sport & Outdoor",
        "products": [
            {"id": f"PROD-SPRT-{i:04d}", "name": n, "price_range": pr, "adult_only": False}
            for i, (n, pr) in enumerate([
                ("Vélo électrique urbain 28\"",     (799.99, 1499.99)),
                ("Tapis de yoga antidérapant",       (24.99,   49.99)),
                ("Haltères hexagonaux 10kg la paire",(39.99,   69.99)),
                ("Raquette de tennis Wilson",        (69.99,  149.99)),
                ("Chaussures trail Salomon",         (99.99,  169.99)),
                ("Gourde inox Klean Kanteen 750ml",  (24.99,   39.99)),
                ("Sac à dos randonnée 40L",          (69.99,  129.99)),
                ("Résistance élastique pack x5",     (14.99,   29.99)),
                ("Cardio-fréquencemètre Polar H10",  (59.99,   89.99)),
                ("Piscine gonflable familiale",      (179.99, 299.99)),
            ], start=1)
        ],
        "adult_only": False,
    },
    "CAT-BEAUTE": {
        "name": "Beauté & Soins",
        "products": [
            {"id": f"PROD-BEAU-{i:04d}", "name": n, "price_range": pr, "adult_only": False}
            for i, (n, pr) in enumerate([
                ("Crème hydratante Clinique",        (39.99,  69.99)),
                ("Sérum Vitamine C The Ordinary",    (9.99,   24.99)),
                ("Mascara waterproof L'Oréal",       (12.99,  22.99)),
                ("Parfum Dior Sauvage EDT 100ml",   (89.99, 119.99)),
                ("Palette fards à paupières Huda",  (49.99,  79.99)),
                ("Tondeuse Braun Series 9",          (89.99, 149.99)),
                ("Brosse à dents électrique Oral-B", (39.99,  79.99)),
                ("Fond de teint Fenty Beauty",       (34.99,  49.99)),
                ("Huile capillaire argan pure",      (14.99,  29.99)),
                ("Coffret soin visage Lancôme",      (79.99, 129.99)),
            ], start=1)
        ],
        "adult_only": False,
    },
    "CAT-LIVRES": {
        "name": "Livres & Culture",
        "products": [
            {"id": f"PROD-LIVR-{i:04d}", "name": n, "price_range": pr, "adult_only": False}
            for i, (n, pr) in enumerate([
                ("Le Problème à Trois Corps — Liu Cixin", (8.99, 19.99)),
                ("Atomic Habits — James Clear",          (14.99, 22.99)),
                ("Harry Potter Intégrale 7 tomes",       (49.99, 79.99)),
                ("Python pour les Data Scientists",      (34.99, 49.99)),
                ("La Psychologie de l'Argent",           (14.99, 22.99)),
                ("Deep Learning — Goodfellow",           (59.99, 89.99)),
                ("Thinking Fast and Slow — Kahneman",    (12.99, 22.99)),
                ("Bande dessinée Astérix Tome 40",        (9.99, 14.99)),
                ("Cahier Leuchtturm A5 pointillés",       (14.99, 22.99)),
                ("Jeux de société Catan",                (34.99, 54.99)),
            ], start=1)
        ],
        "adult_only": False,
    },
    # ── Catégorie ADULTES ─────────────────────────────────────────────────────
    # CAS D'USAGE DIRECTION : Les mineurs et comptes non-vérifiés ne doivent
    # PAS pouvoir acheter ces produits. Le pipeline fraude détecte ces tentatives.
    # Le modèle XGBoost est entraîné sur ces patterns de fraude spécifiques.
    "CAT-ADULT": {
        "name": "Adultes (+18 ans)",
        "products": [
            {"id": f"PROD-ADLT-{i:04d}", "name": n, "price_range": pr, "adult_only": True}
            for i, (n, pr) in enumerate([
                ("Vin rouge Bordeaux Grand Cru 2018",         (24.99,  89.99)),
                ("Whisky Single Malt Glenfiddich 18 ans",    (59.99, 149.99)),
                ("Champagne Moët & Chandon 75cl",             (34.99,  69.99)),
                ("Coffret bières artisanales 12 variétés",    (34.99,  59.99)),
                ("Abonnement streaming adulte — 1 mois",     (12.99,  19.99)),
                ("Roman érotique édition collector",           (14.99,  29.99)),
                ("Jeu de cartes adultes +18",                   (9.99,  24.99)),
                ("Set dégustation spiritueux premium",         (79.99, 199.99)),
                ("Couteau de chasse Laguiole",                 (49.99, 149.99)),
                ("Cigarettes électroniques pack démarrage",   (29.99,  59.99)),
            ], start=1)
        ],
        "adult_only": True,
    },
}

# Liste plate de tous les produits pour tirage aléatoire rapide
ALL_PRODUCTS = []
ADULT_PRODUCTS = []
STANDARD_PRODUCTS = []
for cat_id, cat_data in PRODUCT_CATALOG.items():
    for p in cat_data["products"]:
        p["category_id"] = cat_id
        p["category_name"] = cat_data["name"]
        ALL_PRODUCTS.append(p)
        if p["adult_only"]:
            ADULT_PRODUCTS.append(p)
        else:
            STANDARD_PRODUCTS.append(p)

ALL_PRODUCT_IDS = [p["id"] for p in ALL_PRODUCTS]
PRODUCT_MAP = {p["id"]: p for p in ALL_PRODUCTS}

# =============================================================================
# GÉOGRAPHIE & DÉTECTION D'ANOMALIES
# =============================================================================

# IP ranges légitimes par pays (simplifiés pour la simulation)
LEGIT_IP_BY_COUNTRY = {
    "FR": ["78.", "90.", "91.", "176.", "194.", "82.", "80.", "83."],
    "BE": ["94.", "195.", "212.", "213."],
    "CH": ["85.", "195.", "213.", "217."],
    "DE": ["88.", "91.", "217.", "84.", "213."],
    "IT": ["79.", "151.", "87.", "213."],
    "ES": ["83.", "88.", "213.", "80."],
    "NL": ["83.", "145.", "194.", "37."],
    "LU": ["77.", "89.", "80.", "85."],
    "PT": ["188.", "213.", "83.", "94."],
}

# IPs suspectes utilisées dans les patterns de fraude
# CAS D'USAGE : Ces ranges sont dans la base AbuseIPDB avec score > 70
SUSPICIOUS_IP_RANGES = {
    "TOR_EXIT":     ["185.220.101.", "185.220.100.", "185.107.57.", "193.32.161."],
    "RUSSIA":       ["91.108.", "185.234.218.", "194.87.216.", "185.185.68."],
    "NIGERIA":      ["197.210.", "41.203.", "196.216.", "154.118."],
    "CHINA":        ["218.2.", "222.85.", "61.177.", "116.31."],
    "DATACENTER":   ["45.152.66.", "194.165.16.", "23.95.97.", "162.33.177."],
    "VPN_KNOWN":    ["104.234.220.", "192.241.154.", "45.33.32.", "139.59.1."],
}

def get_legit_ip(country: str) -> str:
    prefixes = LEGIT_IP_BY_COUNTRY.get(country, LEGIT_IP_BY_COUNTRY["FR"])
    prefix = random.choice(prefixes)
    return f"{prefix}{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}"

def get_suspicious_ip(ip_type: str = None) -> str:
    if ip_type is None:
        ip_type = random.choice(list(SUSPICIOUS_IP_RANGES.keys()))
    prefix = random.choice(SUSPICIOUS_IP_RANGES[ip_type])
    return f"{prefix}{random.randint(1, 254)}"

# =============================================================================
# USER AGENTS RÉALISTES
# =============================================================================
# CAS D'USAGE : L'analyse du device_type révèle les patterns d'achat
# (mobile vs desktop), détecte les bots (user-agents anormaux),
# et enrichit les features comportementales du modèle de fraude.

USER_AGENTS = {
    # Mobile iOS (30% du trafic — fort taux de conversion)
    "mobile_ios": [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    ],
    # Mobile Android (35% du trafic)
    "mobile_android": [
        "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 12; Redmi Note 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; OnePlus 12) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36",
    ],
    # Desktop Chrome (20% du trafic — fort panier moyen)
    "desktop_chrome": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.199 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    ],
    # Desktop Firefox (8%)
    "desktop_firefox": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    ],
    # Desktop Safari (5%)
    "desktop_safari": [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ],
    # Bot / Script (2% — utilisé pour simuler les attaques velocity)
    "bot_suspicious": [
        "python-requests/2.31.0",
        "curl/7.88.1",
        "Go-http-client/1.1",
        "Apache-HttpClient/4.5.13 (Java/11.0.19)",
        "Mozilla/5.0 (compatible; MJ12bot/v1.4.8; http://mj12bot.com/)",
    ],
}

DEVICE_WEIGHTS = {
    "mobile_ios":     0.28,
    "mobile_android": 0.33,
    "desktop_chrome": 0.22,
    "desktop_firefox": 0.08,
    "desktop_safari":  0.07,
    "bot_suspicious":  0.02,
}

def get_user_agent() -> tuple[str, str]:
    """Retourne (user_agent_string, device_type)"""
    device_types = list(DEVICE_WEIGHTS.keys())
    weights = list(DEVICE_WEIGHTS.values())
    device_type = random.choices(device_types, weights=weights, k=1)[0]
    ua = random.choice(USER_AGENTS[device_type])
    return ua, device_type

# =============================================================================
# TIMING RÉALISTE — PICS DE TRAFIC
# =============================================================================
# Distribution horaire du trafic e-commerce (données sectorielles)
# CAS D'USAGE : Permet de détecter les anomalies de timing dans la fraude
# (les attaques velocity arrivent souvent la nuit ou tôt le matin)

HOUR_TRAFFIC_WEIGHTS = {
    0: 0.5,   # 00h — nuit, trafic minimal
    1: 0.3,   # 01h
    2: 0.2,   # 02h
    3: 0.2,   # 03h
    4: 0.3,   # 04h
    5: 0.5,   # 05h — début réveil
    6: 1.2,   # 06h
    7: 2.5,   # 07h — commute matin
    8: 3.5,   # 08h
    9: 4.0,   # 09h
    10: 4.5,  # 10h
    11: 4.2,  # 11h
    12: 5.0,  # 12h — pause déjeuner PIC
    13: 4.8,  # 13h
    14: 3.8,  # 14h
    15: 3.5,  # 15h
    16: 3.8,  # 16h
    17: 4.5,  # 17h — fin de journée
    18: 6.5,  # 18h
    19: 8.0,  # 19h — PIC SOIR
    20: 9.5,  # 20h — PIC MAXIMUM
    21: 9.0,  # 21h
    22: 7.5,  # 22h
    23: 5.0,  # 23h
}

def get_realistic_timestamp() -> datetime:
    """
    Génère un timestamp avec distribution horaire réaliste.
    Ajoute une variation de quelques jours dans le passé pour simuler
    des données historiques (pas que le jour J).
    """
    hour = random.choices(
        list(HOUR_TRAFFIC_WEIGHTS.keys()),
        weights=list(HOUR_TRAFFIC_WEIGHTS.values()),
        k=1
    )[0]
    now = datetime.utcnow()
    # Données des 7 derniers jours (réalisme du streaming)
    day_offset = random.choices(range(7), weights=[0.35, 0.20, 0.15, 0.12, 0.08, 0.06, 0.04], k=1)[0]
    from datetime import timedelta
    base = now - timedelta(days=day_offset)
    ts = base.replace(
        hour=hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999999)
    )
    return ts

# =============================================================================
# UTILITAIRES COMMUNS
# =============================================================================

FRENCH_CITIES = [
    ("Paris",       "75001"), ("Paris 16e",   "75016"), ("Lyon",    "69001"),
    ("Marseille",   "13001"), ("Toulouse",    "31000"), ("Nice",    "06000"),
    ("Nantes",      "44000"), ("Strasbourg",  "67000"), ("Montpellier", "34000"),
    ("Bordeaux",    "33000"), ("Lille",       "59000"), ("Rennes",  "35000"),
    ("Reims",       "51100"), ("Le Havre",    "76600"), ("Saint-Étienne", "42000"),
    ("Toulon",      "83000"), ("Grenoble",    "38000"), ("Dijon",   "21000"),
    ("Angers",      "49000"), ("Nîmes",       "30000"), ("Aix-en-Provence", "13100"),
    ("Brest",       "29200"), ("Le Mans",     "72000"), ("Amiens",  "80000"),
    ("Tours",       "37000"), ("Limoges",     "87000"), ("Clermont-Ferrand", "63000"),
    ("Villeurbanne", "69100"), ("Metz",       "57000"), ("Besançon", "25000"),
]

CARRIERS = ["chronopost", "colissimo", "ups", "dhl", "mondial_relay", "fedex", "geodis"]

def maybe_null(value, null_probability: float = 0.03):
    """
    Introduit des valeurs nulles aléatoires pour simuler la vraie vie.
    CAS D'USAGE : Les Great Expectations détectent ces nulls en Bronze
    et les règles Silver les gèrent (valeur par défaut ou rejet en quarantaine).
    null_probability : 3% par défaut (taux réaliste de données manquantes)
    """
    return None if random.random() < null_probability else value

def stable_hash(value: str) -> str:
    """Hash MD5 court stable — utilisé pour les fingerprints et device_id"""
    return hashlib.md5(value.encode()).hexdigest()[:16]