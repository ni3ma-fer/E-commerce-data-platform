"""
=============================================================================
payment_producer.py  —  KiVendTout  |  Étape 2 : Ingestion Kafka
=============================================================================
Producteur Kafka pour les tentatives de transactions de paiement.

CAS D'USAGE DIRECTION ADRESSÉS :
  1. DÉTECTION DE FRAUDE (PRINCIPAL) :
     Ce script implémente 6 patterns de fraude distincts et réalistes.
     Chaque transaction contient un champ `_fraud_scenario` (ground truth
     disponible J+30 via retour bancaire en production) pour entraîner et
     évaluer le modèle XGBoost.

  2. ML CARTE D'IDENTITÉ (KYC) :
     - Tentatives d'achat d'articles CAT-ADULT par des mineurs ou des
       comptes non-vérifiés → Ground truth pour le modèle OCR KYC
     - Lié au champ `id_verification_status` du CRM via `user_id`

  3. GOUVERNANCE RGPD :
     - PAN bancaire JAMAIS en clair (tokenisé dès la source)
     - _is_fraud_ground_truth n'est pas exposé à l'API en production
     - ip_address pseudonymisée en Silver via Presidio

PATTERNS DE FRAUDE IMPLÉMENTÉS :
  P1. ACHAT ADULTE MINEUR     : Mineur qui tente d'acheter sur CAT-ADULT
  P2. ACHAT ADULTE KYC        : Compte non-vérifié sur produit +18
  P3. VELOCITY ATTACK         : N paiements en quelques secondes (carding)
  P4. GEO MISMATCH            : IP Russie / carte FR / livraison Nigéria
  P5. CARD TESTING            : Petits montants répétés (test de carte)
  P6. ACCOUNT TAKEOVER        : Device inconnu + pays inhabituel + montant élevé

ARCHITECTURE :
  Source : Python script (simule le PSP KiVendTout)
  Sink   : Apache Kafka topic "payments-raw" (6 partitions)
  Format : JSON UTF-8, clé = transaction_id
=============================================================================
"""

import json
import time
import random
import uuid
import os
from datetime import datetime, timedelta
from typing import Optional
from confluent_kafka import Producer
from dotenv import load_dotenv

from shared_data_pool import (
    USER_POOL, USER_IDS, MINOR_USER_IDS, ADULT_USER_IDS,
    VERIFIED_USER_IDS, UNVERIFIED_IDS,
    PRODUCT_MAP, ADULT_PRODUCTS, STANDARD_PRODUCTS, ALL_PRODUCTS,
    get_legit_ip, get_suspicious_ip, get_user_agent,
    get_realistic_timestamp, maybe_null, stable_hash,
    LEGIT_IP_BY_COUNTRY,
)

load_dotenv("../docker/.env")

# ─── CONFIGURATION KAFKA ──────────────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "client.id":         "payment-producer-kivendtout-v2",
    "acks":              "all",
    "retries":           10,            # Paiements critiques → max retry
    "retry.backoff.ms":  500,
    "compression.type":  "snappy",
    "delivery.timeout.ms": 30000,
}
TOPIC = "payments-raw"

producer = Producer(KAFKA_CONFIG)

# ─── MÉTHODES DE PAIEMENT ─────────────────────────────────────────────────────
PAYMENT_METHODS = {
    "card_visa":       0.32,
    "card_mastercard": 0.28,
    "paypal":          0.18,
    "apple_pay":       0.10,
    "google_pay":      0.07,
    "card_amex":       0.03,
    "virement_banque": 0.02,
}

# Banques émettrices par pays (BIN simulés pour le feature engineering)
CARD_ISSUERS_BY_COUNTRY = {
    "FR": ["BNP Paribas", "Crédit Agricole", "Société Générale", "LCL",
           "Caisse d'Épargne", "Boursorama", "Revolut FR", "N26 FR"],
    "BE": ["ING Belgium", "BNP Paribas Fortis", "KBC", "Belfius"],
    "CH": ["UBS", "Credit Suisse", "PostFinance", "Raiffeisen CH"],
    "DE": ["Deutsche Bank", "Commerzbank", "Sparkasse", "N26 DE"],
    "IT": ["UniCredit", "Intesa Sanpaolo", "Banca Monte dei Paschi"],
    "RU": ["Sberbank", "VTB Bank", "Alfa-Bank", "Tinkoff"],   # Fraude géo
    "NG": ["Access Bank", "GTBank", "Zenith Bank", "First Bank NG"],
    "CN": ["ICBC", "Bank of China", "China Construction Bank"],
}

# Devises par pays (feature pour détecter les incohérences)
CURRENCY_BY_COUNTRY = {
    "FR": "EUR", "BE": "EUR", "DE": "EUR", "IT": "EUR",
    "ES": "EUR", "NL": "EUR", "LU": "EUR", "PT": "EUR",
    "CH": "CHF", "GB": "GBP", "US": "USD",
    "RU": "RUB", "NG": "NGN", "CN": "CNY",
}


# =============================================================================
# GÉNÉRATEURS DE TRANSACTIONS — COMPORTEMENT NORMAL
# =============================================================================

def generate_normal_transaction(user_id: str) -> dict:
    """
    Transaction légitime d'un utilisateur normal.
    
    Représente ~92% des transactions. Sert de référence pour le modèle
    XGBoost : toutes les features normales sans anomalies.
    """
    user = USER_POOL[user_id]
    country = user["country"]
    
    # Sélection produit adapté à l'utilisateur
    if user["kyc_status"] == "verified" and user["age"] >= 18 and random.random() < 0.08:
        product = random.choice(ADULT_PRODUCTS + STANDARD_PRODUCTS)
    else:
        product = random.choice(STANDARD_PRODUCTS)
    
    amount = round(random.uniform(*product["price_range"]), 2)
    
    # Panier moyen par segment (cohérence avec le CRM)
    segment_multiplier = {"bronze": 1.0, "silver": 1.3, "gold": 1.8, "vip": 2.5}
    amount = round(amount * segment_multiplier.get(user["segment"], 1.0), 2)
    
    ua, device = get_user_agent()
    ip = get_legit_ip(country)
    ts = get_realistic_timestamp()
    
    return {
        "transaction_id":         str(uuid.uuid4()),
        "user_id":                user_id,
        "amount":                 amount,
        "currency":               CURRENCY_BY_COUNTRY.get(country, "EUR"),
        "payment_method":         random.choices(
            list(PAYMENT_METHODS.keys()),
            weights=list(PAYMENT_METHODS.values()), k=1
        )[0],
        "product_id":             product["id"],
        "product_category_id":    product["category_id"],
        "is_adult_product":       product["adult_only"],
        "card_issuer":            maybe_null(
            random.choice(CARD_ISSUERS_BY_COUNTRY.get(country, CARD_ISSUERS_BY_COUNTRY["FR"])),
            null_probability=0.05
        ),
        "billing_country":        country,
        "shipping_country":       country,   # Cohérent : livraison = facturation
        "ip_address":             ip,
        "ip_country_inferred":    country,   # Cohérent avec billing_country
        "device_type":            device,
        "user_agent":             ua,
        "device_fingerprint":     stable_hash(f"{user_id}-{device}"),
        "is_new_device":          maybe_null(random.random() < 0.08, null_probability=0.02),
        "is_new_merchant":        False,
        "user_kyc_status":        user["kyc_status"],
        "user_age":               user["age"],
        "timestamp":              ts.isoformat() + "Z",
        "merchant_id":            f"MERCH-{random.randint(1, 200):04d}",
        # Ground truth (disponible J+30 via chargeback en production)
        "_is_fraud":              False,
        "_fraud_scenario":        "none",
        "_fraud_confidence":      0.0,
    }


# =============================================================================
# PATTERNS DE FRAUDE COMPLEXES
# =============================================================================

def generate_fraud_p1_adult_minor(user_id: Optional[str] = None) -> dict:
    """
    PATTERN P1 : Tentative d'achat article +18 par un MINEUR
    
    Scénario réel : Un adolescent de 15 ans essaie d'acheter du whisky ou
    un contenu adulte. Le KYC n'a pas bloqué en amont (statut 'pending' ou 'none').
    
    CAS D'USAGE ML :
      - Ground truth pour le modèle XGBoost feature "is_adult_product + age < 18"
      - Corréler avec clickstream "adult_content_blocked" pour évaluer le pipeline
      - Entraîner le modèle OCR à détecter les mineurs sur les CNI
    
    FEATURES CARACTÉRISTIQUES :
      - is_adult_product = True
      - user_age < 18
      - user_kyc_status IN ('pending', 'none', 'rejected')
    """
    if user_id is None:
        user_id = random.choice(MINOR_USER_IDS)
    
    user = USER_POOL[user_id]
    product = random.choice(ADULT_PRODUCTS)
    amount = round(random.uniform(*product["price_range"]), 2)
    country = user["country"]
    ua, device = get_user_agent()
    ts = get_realistic_timestamp()
    
    return {
        "transaction_id":         str(uuid.uuid4()),
        "user_id":                user_id,
        "amount":                 amount,
        "currency":               CURRENCY_BY_COUNTRY.get(country, "EUR"),
        "payment_method":         random.choices(
            list(PAYMENT_METHODS.keys()), weights=list(PAYMENT_METHODS.values()), k=1
        )[0],
        "product_id":             product["id"],
        "product_category_id":    product["category_id"],
        "is_adult_product":       True,
        "card_issuer":            random.choice(CARD_ISSUERS_BY_COUNTRY.get(country, ["Unkn."])),
        "billing_country":        country,
        "shipping_country":       country,
        "ip_address":             get_legit_ip(country),
        "ip_country_inferred":    country,
        "device_type":            device,
        "user_agent":             ua,
        "device_fingerprint":     stable_hash(f"{user_id}-{device}"),
        "is_new_device":          random.random() < 0.25,
        "is_new_merchant":        True,   # Première fois sur ce marchand adulte
        "user_kyc_status":        user["kyc_status"],  # 'pending', 'none', ou 'rejected'
        "user_age":               user["age"],          # < 18 — KEY FEATURE
        "timestamp":              ts.isoformat() + "Z",
        "merchant_id":            f"MERCH-ADULT-{random.randint(1, 10):03d}",
        "_is_fraud":              True,
        "_fraud_scenario":        "adult_product_minor",
        "_fraud_confidence":      round(random.uniform(0.82, 0.99), 3),
    }


def generate_fraud_p2_adult_kyc_bypass(user_id: Optional[str] = None) -> dict:
    """
    PATTERN P2 : Achat article +18 par compte non-vérifié (KYC bypass)
    
    Scénario : Un adulte de 22 ans a créé un compte mais n'a jamais soumis
    sa CNI (kyc_status = 'none' ou 'pending'). Il tente quand même d'acheter
    un produit réservé aux adultes vérifiés.
    
    DIFFÉRENCE P1 vs P2 : L'utilisateur EST majeur mais son identité n'est
    pas vérifiée. Le pipeline doit bloquer dans les deux cas mais les features
    ML sont différentes (age ≥ 18 mais kyc non conforme).
    """
    if user_id is None:
        # Adultes NON vérifiés
        adult_unverified = [uid for uid in UNVERIFIED_IDS if USER_POOL[uid]["age"] >= 18]
        if not adult_unverified:
            adult_unverified = ADULT_USER_IDS[:100]
        user_id = random.choice(adult_unverified)
    
    user = USER_POOL[user_id]
    product = random.choice(ADULT_PRODUCTS)
    amount = round(random.uniform(*product["price_range"]), 2)
    country = user["country"]
    ua, device = get_user_agent()
    ts = get_realistic_timestamp()
    
    return {
        "transaction_id":         str(uuid.uuid4()),
        "user_id":                user_id,
        "amount":                 amount,
        "currency":               CURRENCY_BY_COUNTRY.get(country, "EUR"),
        "payment_method":         random.choice(["card_visa", "card_mastercard", "paypal"]),
        "product_id":             product["id"],
        "product_category_id":    product["category_id"],
        "is_adult_product":       True,
        "card_issuer":            random.choice(CARD_ISSUERS_BY_COUNTRY.get(country, ["Unknown"])),
        "billing_country":        country,
        "shipping_country":       country,
        "ip_address":             get_legit_ip(country),
        "ip_country_inferred":    country,
        "device_type":            device,
        "user_agent":             ua,
        "device_fingerprint":     stable_hash(f"{user_id}-{device}"),
        "is_new_device":          False,
        "is_new_merchant":        True,
        "user_kyc_status":        user["kyc_status"],  # 'none' ou 'pending' — KEY FEATURE
        "user_age":               user["age"],          # ≥ 18
        "timestamp":              ts.isoformat() + "Z",
        "merchant_id":            f"MERCH-ADULT-{random.randint(1, 10):03d}",
        "_is_fraud":              True,
        "_fraud_scenario":        "adult_product_kyc_bypass",
        "_fraud_confidence":      round(random.uniform(0.71, 0.89), 3),
    }


def generate_fraud_p3_velocity_attack(user_id: Optional[str] = None) -> list[dict]:
    """
    PATTERN P3 : Velocity Attack (attaque par vitesse)
    
    Scénario : Un fraudeur a obtenu des données de carte et effectue
    N transactions en quelques secondes pour tester la validité des numéros
    avant de faire de gros achats (carding attack).
    
    RETOURNE UNE LISTE de transactions (burst de 4 à 8 transactions).
    
    CAS D'USAGE ML :
      - Feature critique : nb_transactions / 5 minutes
      - Feature : variance des montants dans la fenêtre
      - Feature : nb_merchants_distincts dans la fenêtre
      - Latence de détection requise : < 200ms (avant validation bancaire)
    
    FEATURES CARACTÉRISTIQUES :
      - 4-8 transactions en moins de 60 secondes
      - Même IP, même device_fingerprint
      - Montants variés (test de plafond)
      - Marchands différents
    """
    if user_id is None:
        user_id = random.choice(USER_IDS)
    
    user = USER_POOL[user_id]
    country = user["country"]
    ua, device = get_user_agent()
    # IP suspecte pour l'attaque velocity (souvent depuis un datacenter)
    attack_ip = get_suspicious_ip("DATACENTER")
    
    n_transactions = random.randint(4, 8)
    base_time = get_realistic_timestamp()
    
    transactions = []
    # Montants de test caractéristiques du carding : petits d'abord, puis croissants
    test_amounts = sorted([round(random.uniform(0.99, 9.99), 2) for _ in range(n_transactions)])
    
    for i in range(n_transactions):
        # Chaque transaction arrive quelques secondes après la précédente
        ts = base_time + timedelta(seconds=i * random.randint(3, 15))
        product = random.choice(STANDARD_PRODUCTS)
        
        transactions.append({
            "transaction_id":         str(uuid.uuid4()),
            "user_id":                user_id,
            "amount":                 test_amounts[i],  # Montants croissants = test de carte
            "currency":               "EUR",
            "payment_method":         "card_visa",       # Visa souvent utilisé pour le carding
            "product_id":             product["id"],
            "product_category_id":    product["category_id"],
            "is_adult_product":       False,
            "card_issuer":            random.choice(["Unknown Issuer", "Prepaid Card", "Virtual Card"]),
            "billing_country":        country,           # Pays du compte compromis
            "shipping_country":       country,
            "ip_address":             attack_ip,         # IP datacenter — KEY FEATURE
            "ip_country_inferred":    "US",              # IP ≠ billing_country — KEY FEATURE
            "device_type":            device,
            "user_agent":             ua,
            "device_fingerprint":     stable_hash(f"{user_id}-{device}"),  # MÊME fingerprint
            "is_new_device":          i == 0,            # Premier = nouveau device
            "is_new_merchant":        True,              # Marchands différents à chaque fois
            "user_kyc_status":        user["kyc_status"],
            "user_age":               user["age"],
            "timestamp":              ts.isoformat() + "Z",
            "merchant_id":            f"MERCH-{random.randint(1, 200):04d}",  # Marchands variés
            "velocity_attack_group":  str(uuid.uuid4())[:8],  # Même groupe = même attaque
            "_is_fraud":              True,
            "_fraud_scenario":        "velocity_attack",
            "_fraud_confidence":      round(random.uniform(0.88, 0.99), 3),
        })
    
    return transactions


def generate_fraud_p4_geo_mismatch(user_id: Optional[str] = None) -> dict:
    """
    PATTERN P4 : Incohérence géographique (GeoMismatch)
    
    Scénario classique : Carte bancaire française (billing_country = FR),
    utilisateur connecté depuis une IP russe ou via Tor, livraison au Nigéria.
    Typique d'un compte compromis utilisé par un réseau de fraude international.
    
    CAS D'USAGE ML :
      - Feature : distance_km(ip_geoloc, billing_address)
      - Feature : ip_is_vpn_or_tor (via MaxMind GeoLite2)
      - Feature : billing_country != shipping_country
      - Feature : ip_reputation_score > 70 (via AbuseIPDB API)
    
    FEATURES CARACTÉRISTIQUES :
      - billing_country = "FR" (carte française)
      - shipping_country IN ("NG", "CN", "RU") — livraison suspecte
      - ip_country_inferred IN ("RU", "CN") OU ip_type = "TOR_EXIT"
      - Montant élevé (le fraudeur maximise)
    """
    if user_id is None:
        user_id = random.choice(ADULT_USER_IDS)
    
    user = USER_POOL[user_id]
    ua, device = get_user_agent()
    ts = get_realistic_timestamp()
    
    # Configuration de l'incohérence géographique
    fraud_scenario = random.choice([
        # (ip_type, ip_inferred_country, shipping_country, description)
        ("TOR_EXIT",   "TOR",  "NG", "Tor + livraison Nigeria"),
        ("RUSSIA",     "RU",   "CN", "IP Russie + livraison Chine"),
        ("DATACENTER", "US",   "NG", "Datacenter US + livraison Nigeria"),
        ("NIGERIA",    "NG",   "NG", "IP Nigeria + carte FR"),
        ("VPN_KNOWN",  "NL",   "RU", "VPN Pays-Bas + livraison Russie"),
    ])
    ip_type, ip_country, ship_country, description = fraud_scenario
    
    # Montant élevé : les fraudeurs maximisent quand ils ont accès à une carte
    amount = round(random.uniform(199.99, 899.99), 2)
    product = random.choice([p for p in ALL_PRODUCTS if p["price_range"][1] > 100])
    
    return {
        "transaction_id":         str(uuid.uuid4()),
        "user_id":                user_id,
        "amount":                 amount,
        "currency":               "EUR",       # Carte FR donc EUR
        "payment_method":         random.choice(["card_visa", "card_mastercard"]),
        "product_id":             product["id"],
        "product_category_id":    product["category_id"],
        "is_adult_product":       product["adult_only"],
        "card_issuer":            random.choice(CARD_ISSUERS_BY_COUNTRY["FR"]),
        "billing_country":        "FR",        # Carte française
        "shipping_country":       ship_country, # LIVRAISON PAYS SUSPECT — KEY FEATURE
        "ip_address":             get_suspicious_ip(ip_type),
        "ip_country_inferred":    ip_country,  # IP ≠ billing_country — KEY FEATURE
        "device_type":            device,
        "user_agent":             ua,
        "device_fingerprint":     stable_hash(f"{user_id}-unknown-{ts.hour}"),  # Device inconnu
        "is_new_device":          True,        # NOUVEAU device — KEY FEATURE
        "is_new_merchant":        True,
        "user_kyc_status":        user["kyc_status"],
        "user_age":               user["age"],
        "timestamp":              ts.isoformat() + "Z",
        "merchant_id":            f"MERCH-{random.randint(1, 200):04d}",
        "geo_mismatch_detail":    description,
        "_is_fraud":              True,
        "_fraud_scenario":        "geo_mismatch",
        "_fraud_confidence":      round(random.uniform(0.85, 0.98), 3),
    }


def generate_fraud_p5_card_testing(user_id: Optional[str] = None) -> list[dict]:
    """
    PATTERN P5 : Card Testing (test de carte volée)
    
    Scénario : Un fraudeur a acheté un lot de numéros de carte sur le darkweb.
    Il teste chaque carte avec un micro-paiement (0.50€ à 2.00€) pour vérifier
    quelles cartes sont encore actives avant de faire de grosses transactions.
    
    DIFFÉRENCE P3 vs P5 :
      - P3 (Velocity) : UN seul user, BEAUCOUP de transactions rapides
      - P5 (Card Testing) : BEAUCOUP de users différents, petits montants identiques
      → Feature différente : même IP, users différents, montants quasi-identiques
    """
    if user_id is None:
        user_id = random.choice(USER_IDS)
    
    user = USER_POOL[user_id]
    ts = get_realistic_timestamp()
    # IP unique pour tout le batch de tests (même attaquant)
    attack_ip = get_suspicious_ip("VPN_KNOWN")
    ua, device = get_user_agent()
    
    n_cards_tested = random.randint(3, 6)
    transactions = []
    
    for i in range(n_cards_tested):
        card_uid = random.choice(USER_IDS)  # Cartes de différents utilisateurs
        test_amount = round(random.uniform(0.50, 2.99), 2)  # Micro-paiement = test
        ts_i = ts + timedelta(seconds=i * random.randint(10, 45))
        
        transactions.append({
            "transaction_id":         str(uuid.uuid4()),
            "user_id":                card_uid,    # USER DIFFÉRENT à chaque fois
            "amount":                 test_amount, # MICRO-MONTANT — KEY FEATURE
            "currency":               "EUR",
            "payment_method":         "card_visa",
            "product_id":             "PROD-LIVR-0009",  # Produit peu cher
            "product_category_id":    "CAT-LIVRES",
            "is_adult_product":       False,
            "card_issuer":            "Virtual Card",
            "billing_country":        USER_POOL[card_uid]["country"],
            "shipping_country":       USER_POOL[card_uid]["country"],
            "ip_address":             attack_ip,          # MÊME IP — KEY FEATURE
            "ip_country_inferred":    "NL",               # VPN Pays-Bas
            "device_type":            device,
            "user_agent":             ua,                  # MÊME user-agent — KEY FEATURE
            "device_fingerprint":     stable_hash(f"attacker-device-{attack_ip}"),
            "is_new_device":          True,
            "is_new_merchant":        False,
            "user_kyc_status":        USER_POOL[card_uid]["kyc_status"],
            "user_age":               USER_POOL[card_uid]["age"],
            "timestamp":              ts_i.isoformat() + "Z",
            "merchant_id":            "MERCH-0001",  # Même marchand = card testing
            "card_testing_batch_id":  stable_hash(attack_ip + str(ts.date())),
            "_is_fraud":              True,
            "_fraud_scenario":        "card_testing",
            "_fraud_confidence":      round(random.uniform(0.79, 0.95), 3),
        })
    
    return transactions


def generate_fraud_p6_account_takeover(user_id: Optional[str] = None) -> dict:
    """
    PATTERN P6 : Account Takeover (prise de contrôle de compte)
    
    Scénario : Un attaquant a obtenu les identifiants d'un compte légitime
    (phishing, credential stuffing). Il se connecte depuis un pays inhabituel,
    sur un device jamais vu, et effectue un achat élevé rapidement.
    
    CAS D'USAGE ML :
      - Feature : is_new_device = True + montant >> user_avg_amount_30d
      - Feature : ip_country != user_usual_country
      - Feature : pas de session clickstream préalable (achat direct)
      - Corrélation avec les logs de connexion (is_new_device + nouveau pays)
    """
    if user_id is None:
        # Cible préférentielle : VIP et Gold (comptes à fort historique)
        vip_gold = [uid for uid, u in USER_POOL.items() if u["segment"] in ("vip", "gold")]
        user_id = random.choice(vip_gold)
    
    user = USER_POOL[user_id]
    ts = get_realistic_timestamp()
    ua, device = get_user_agent()
    
    # Pays d'attaque ≠ pays habituel de l'utilisateur
    usual_country = user["country"]
    attack_countries = ["RU", "CN", "NG", "BR", "VN"]
    attack_ip_type = random.choice(["RUSSIA", "CHINA", "NIGERIA", "DATACENTER"])
    attack_ip = get_suspicious_ip(attack_ip_type)
    
    # Produit électronique haut de gamme (revente facile)
    electronics = [p for p in ALL_PRODUCTS if p["category_id"] == "CAT-ELEC"
                   and p["price_range"][1] > 200]
    product = random.choice(electronics) if electronics else random.choice(ALL_PRODUCTS)
    
    # Montant maximal (le fraudeur maximise avant que le vrai propriétaire réagisse)
    amount = round(product["price_range"][1] * random.uniform(0.9, 1.0), 2)
    
    return {
        "transaction_id":         str(uuid.uuid4()),
        "user_id":                user_id,
        "amount":                 amount,          # Montant élevé >> moyenne — KEY FEATURE
        "currency":               CURRENCY_BY_COUNTRY.get(usual_country, "EUR"),
        "payment_method":         "card_visa",
        "product_id":             product["id"],
        "product_category_id":    product["category_id"],
        "is_adult_product":       False,
        "card_issuer":            random.choice(CARD_ISSUERS_BY_COUNTRY.get(usual_country, ["Unknown"])),
        "billing_country":        usual_country,  # Carte légitime du propriétaire
        "shipping_country":       random.choice(["RU", "CN", "NG"]),  # Livraison suspecte
        "ip_address":             attack_ip,       # IP attaquant — KEY FEATURE
        "ip_country_inferred":    attack_ip_type.title()[:2],
        "device_type":            device,
        "user_agent":             ua,
        "device_fingerprint":     stable_hash(f"unknown-attacker-{uuid.uuid4()}"),
        "is_new_device":          True,            # JAMAIS VU ce device — KEY FEATURE
        "is_new_merchant":        True,
        "user_kyc_status":        user["kyc_status"],
        "user_age":               user["age"],
        "timestamp":              ts.isoformat() + "Z",
        "merchant_id":            f"MERCH-{random.randint(1, 200):04d}",
        "account_takeover_indicator": "new_device_new_country_high_amount",
        "_is_fraud":              True,
        "_fraud_scenario":        "account_takeover",
        "_fraud_confidence":      round(random.uniform(0.80, 0.97), 3),
    }


# =============================================================================
# DISTRIBUTION DES TRANSACTIONS
# =============================================================================

# Distribution réaliste des cas (basée sur les données sectorielles)
TRANSACTION_SCENARIOS = {
    "normal":               0.920,   # 92% de transactions légitimes
    "fraud_p1_minor":       0.010,   # 1.0% — mineurs sur produits adultes
    "fraud_p2_kyc_bypass":  0.015,   # 1.5% — adultes non-vérifiés sur produits adultes
    "fraud_p3_velocity":    0.015,   # 1.5% — velocity attack
    "fraud_p4_geo_mismatch":0.020,   # 2.0% — incohérence géographique
    "fraud_p5_card_testing":0.010,   # 1.0% — card testing
    "fraud_p6_ato":         0.010,   # 1.0% — account takeover
}

# =============================================================================
# PRODUCTEUR PRINCIPAL
# =============================================================================

def delivery_callback(err, msg):
    if err is not None:
        print(f"[CRITICAL] Payment delivery FAILED for {msg.key()}: {err}")


def run_producer(
    events_per_minute: int = 80,
    duration_seconds:  int = 600,
    verbose:           bool = True
):
    """
    Lance le producteur de paiements.
    
    Taux frauduleux global : ~8% (supérieur à la réalité ~0.5% pour entraîner
    le modèle avec suffisamment d'exemples positifs en développement).
    En production, les poids seraient ajustés à la réalité sectorielle.
    """
    scenarios = list(TRANSACTION_SCENARIOS.keys())
    weights   = list(TRANSACTION_SCENARIOS.values())
    
    interval = 60.0 / events_per_minute
    start_time = time.time()
    total_sent = 0
    fraud_counts = {s: 0 for s in scenarios}
    
    print(f"[INFO] Démarrage producteur paiements | {events_per_minute} txn/min")
    print(f"[INFO] Distribution fraude: {sum(v for k,v in TRANSACTION_SCENARIOS.items() if k != 'normal'):.1%}")
    
    while time.time() - start_time < duration_seconds:
        scenario = random.choices(scenarios, weights=weights, k=1)[0]
        
        transactions = []  # Peut être une liste (velocity, card testing)
        
        if scenario == "normal":
            user_id = random.choice(USER_IDS)
            transactions = [generate_normal_transaction(user_id)]
        
        elif scenario == "fraud_p1_minor":
            if MINOR_USER_IDS:
                transactions = [generate_fraud_p1_adult_minor()]
        
        elif scenario == "fraud_p2_kyc_bypass":
            transactions = [generate_fraud_p2_adult_kyc_bypass()]
        
        elif scenario == "fraud_p3_velocity":
            user_id = random.choice(USER_IDS)
            transactions = generate_fraud_p3_velocity_attack(user_id)
            # Les transactions velocity arrivent TOUTES en burst → pas de sleep inter-txn
        
        elif scenario == "fraud_p4_geo_mismatch":
            transactions = [generate_fraud_p4_geo_mismatch()]
        
        elif scenario == "fraud_p5_card_testing":
            transactions = generate_fraud_p5_card_testing()
        
        elif scenario == "fraud_p6_ato":
            transactions = [generate_fraud_p6_account_takeover()]
        
        for txn in transactions:
            # Supprimer les champs internes non destinés à Kafka Bronze
            # (le ground truth est stocké séparément en base de données)
            payload = {k: v for k, v in txn.items() if not k.startswith("_")}
            # Mais on garde _fraud_scenario pour le feature store Silver
            # (disponible J+30 en production, immédiatement en dev)
            payload["fraud_label_ground_truth"] = txn["_is_fraud"]
            payload["fraud_scenario_label"]     = txn["_fraud_scenario"]
            
            producer.produce(
                topic=TOPIC,
                key=txn["transaction_id"],
                value=json.dumps(payload, ensure_ascii=False, default=str),
                callback=delivery_callback
            )
            total_sent += 1
        
        fraud_counts[scenario] += 1
        
        if total_sent % 50 == 0:
            producer.poll(0)
        
        if verbose and total_sent % 100 == 0:
            elapsed = time.time() - start_time
            fraud_total = sum(v for k, v in fraud_counts.items() if k != "normal")
            print(
                f"[INFO] Transactions: {total_sent:,} | "
                f"Fraudes: {fraud_total} ({fraud_total/max(total_sent,1)*100:.1f}%) | "
                f"Elapsed: {elapsed:.0f}s"
            )
        
        # Velocity attack : pas de sleep (burst naturel)
        if scenario not in ("fraud_p3_velocity", "fraud_p5_card_testing"):
            time.sleep(interval)
    
    producer.flush(timeout=30)
    
    print(f"\n[OK] Producteur paiements terminé")
    print(f"  Total transactions   : {total_sent:,}")
    for scenario, count in fraud_counts.items():
        print(f"  {scenario:<30} : {count:,}")


if __name__ == "__main__":
    run_producer(
        events_per_minute=60,
        duration_seconds=600,
        verbose=True
    )