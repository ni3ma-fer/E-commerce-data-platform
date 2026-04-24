"""
=============================================================================
clickstream_producer.py  —  KiVendTout  |  Étape 2 : Ingestion Kafka
=============================================================================
Producteur Kafka pour les événements de navigation et d'interaction utilisateur.

CAS D'USAGE DIRECTION ADRESSÉS :
  1. ANALYSE COMPORTEMENTALE :
     - Sessions réalistes avec tunnel de conversion (home → categorie → produit
       → panier → checkout → confirmation)
     - Détection des abandons de panier (taux réel ~70%)
     - Attribution marketing (referrer → source → taux de conversion)

  2. KYC / VÉRIFICATION IDENTITÉ :
     - Événements "id_verification_started" et "id_verification_failed"
       tracés dans le clickstream pour corréler avec le pipeline OCR
     - Les utilisateurs mineurs qui tentent d'accéder à CAT-ADULT
       génèrent des événements spécifiques pour l'entraînement ML

  3. FRAUDE :
     - Comportements de navigation anormaux (pas de page produit avant panier)
     - Devices suspects (bots, user-agents automatisés)
     - Sessions multi-devices pour un même user (indicateur de fraude)

ARCHITECTURE :
  Source : Python script (simule le SDK JavaScript du site)
  Sink   : Apache Kafka topic "clickstream-raw" (6 partitions)
  Format : JSON UTF-8, clé = user_id (garantit l'ordre par utilisateur)
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

# Import du pool partagé — cohérence relationnelle inter-scripts
from shared_data_pool import (
    USER_POOL, USER_IDS, ADULT_USER_IDS, MINOR_USER_IDS,
    PRODUCT_CATALOG, ADULT_PRODUCTS, STANDARD_PRODUCTS, PRODUCT_MAP,
    get_user_agent, get_legit_ip, get_realistic_timestamp, maybe_null,
    stable_hash, LEGIT_IP_BY_COUNTRY
)

load_dotenv("../docker/.env")

# ─── CONFIGURATION KAFKA ──────────────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "client.id":         "clickstream-producer-kivendtout-v2",
    "acks":              "all",
    "retries":           5,
    "retry.backoff.ms":  1000,
    "compression.type":  "snappy",        # Réduit la bande passante de ~40%
    "linger.ms":         5,               # Batch micro pour le débit
    "batch.size":        16384,
}
TOPIC = "clickstream-raw"

producer = Producer(KAFKA_CONFIG)

# ─── PAGES ET NAVIGATION ──────────────────────────────────────────────────────

# Structure hiérarchique du site (reflète l'arborescence réelle)
SITE_PAGES = {
    "homepage": {
        "path": "/",
        "next_likely": ["category_listing", "search_results", "homepage"],
        "weights":     [0.45, 0.35, 0.20],
    },
    "category_listing": {
        "path": "/categories/{cat_id}",
        "next_likely": ["product_detail", "category_listing", "search_results", "homepage"],
        "weights":     [0.55, 0.20, 0.15, 0.10],
    },
    "product_detail": {
        "path": "/produits/{product_id}",
        "next_likely": ["cart_add", "product_detail", "category_listing", "homepage"],
        "weights":     [0.30, 0.25, 0.30, 0.15],
    },
    "search_results": {
        "path": "/recherche",
        "next_likely": ["product_detail", "category_listing", "search_results", "homepage"],
        "weights":     [0.45, 0.25, 0.20, 0.10],
    },
    "cart_view": {
        "path": "/panier",
        "next_likely": ["checkout_start", "product_detail", "homepage"],
        "weights":     [0.35, 0.45, 0.20],  # 65% d'abandon panier = réaliste
    },
    "checkout_start": {
        "path": "/commande/livraison",
        "next_likely": ["checkout_payment", "cart_view", "homepage"],
        "weights":     [0.65, 0.25, 0.10],
    },
    "checkout_payment": {
        "path": "/commande/paiement",
        "next_likely": ["order_confirmation", "checkout_start"],
        "weights":     [0.80, 0.20],
    },
    "order_confirmation": {
        "path": "/commande/confirmation",
        "next_likely": ["homepage", "product_detail"],
        "weights":     [0.70, 0.30],
    },
    "user_profile": {
        "path": "/compte/profil",
        "next_likely": ["user_orders", "user_kyc", "homepage"],
        "weights":     [0.40, 0.30, 0.30],
    },
    "user_orders": {
        "path": "/compte/commandes",
        "next_likely": ["user_profile", "product_detail", "homepage"],
        "weights":     [0.40, 0.35, 0.25],
    },
    "user_kyc": {
        "path": "/compte/verification-identite",
        "next_likely": ["user_profile", "homepage"],
        "weights":     [0.60, 0.40],
    },
}

# Événements spéciaux (ne font pas partie du tunnel mais sont cruciaux)
SPECIAL_EVENTS = [
    "id_verification_started",   # KYC : début du upload CNI
    "id_verification_failed",    # KYC : OCR a échoué ou mineur détecté
    "id_verification_success",   # KYC : vérification réussie
    "age_gate_shown",            # Affichage du prompt "Êtes-vous majeur ?"
    "age_gate_rejected",         # L'utilisateur a déclaré être mineur (rare mais existe)
    "adult_content_blocked",     # Tentative accès CAT-ADULT par non-vérifié
    "newsletter_subscribe",      # Consentement marketing
    "wishlist_add",              # Ajout à la liste de souhaits
    "product_share",             # Partage produit sur réseau social
    "coupon_applied",            # Code promo appliqué
    "coupon_invalid",            # Code promo invalide
    "search_no_results",         # Recherche sans résultats (signal UX)
]

# Sources de trafic (attribution marketing)
REFERRERS = {
    "google_organic":    0.28,
    "direct":            0.22,
    "google_ads":        0.15,
    "facebook_ads":      0.10,
    "email_campaign":    0.09,
    "instagram":         0.06,
    "affiliate":         0.04,
    "bing_organic":      0.03,
    "tiktok":            0.02,
    "youtube":           0.01,
}


# =============================================================================
# LOGIQUE DE SESSION
# =============================================================================

class UserSession:
    """
    Modélise une session utilisateur réaliste.
    
    CONTRAINTE MÉTIER : Un utilisateur ne peut pas mettre un produit au panier
    sans avoir vu la page produit. Cette contrainte simule le comportement réel
    et rend les features comportementales du modèle ML fiables.
    
    CAS D'USAGE KYC : Si un utilisateur non-vérifié tente d'accéder à un
    produit adulte, des événements spécifiques sont générés pour l'entraînement
    du modèle de détection de fraude.
    """
    
    def __init__(self, user_id: str):
        self.session_id       = str(uuid.uuid4())[:12]
        self.user_id          = user_id
        self.user             = USER_POOL[user_id]
        self.ua, self.device  = get_user_agent()
        self.ip               = get_legit_ip(self.user["country"])
        self.session_start    = get_realistic_timestamp()
        self.current_page     = "homepage"
        self.events_count     = 0
        self.viewed_products  = []    # CONTRAINTE : must see before cart
        self.carted_products  = []
        self.referrer         = random.choices(
            list(REFERRERS.keys()), weights=list(REFERRERS.values()), k=1
        )[0]
        self.is_bot           = self.device == "bot_suspicious"
        # Durée de session : bots courts, humains plus longs
        self.max_events       = random.randint(2, 8) if self.is_bot else random.randint(3, 18)
        self.completed_order  = False
        # Fingerprint stable par user+device (feature fraude)
        self.device_fingerprint = stable_hash(f"{user_id}-{self.device}-{self.ua[:30]}")
    
    def get_elapsed_seconds(self) -> int:
        """Temps écoulé depuis le début de la session"""
        return int((datetime.utcnow() - self.session_start).total_seconds())
    
    def get_current_timestamp(self) -> str:
        offset_seconds = self.events_count * random.randint(5, 120)
        ts = self.session_start + timedelta(seconds=offset_seconds)
        return ts.isoformat() + "Z"


def generate_page_event(session: UserSession, page: str,
                        product_id: Optional[str] = None,
                        category_id: Optional[str] = None) -> dict:
    """Génère un événement de navigation standard"""
    
    page_info = SITE_PAGES.get(page, SITE_PAGES["homepage"])
    path = page_info["path"]
    if product_id:
        path = path.replace("{product_id}", product_id)
    if category_id:
        path = path.replace("{cat_id}", category_id)
    
    return {
        "event_id":           str(uuid.uuid4()),
        "event_type":         "page_view",
        "page":               page,
        "page_path":          path,
        "user_id":            session.user_id,
        "session_id":         session.session_id,
        "timestamp":          session.get_current_timestamp(),
        "device_type":        session.device,
        "user_agent":         session.ua,
        "device_fingerprint": session.device_fingerprint,
        "ip_address":         session.ip,
        "referrer":           session.referrer if session.events_count == 0 else "internal",
        "product_id":         maybe_null(product_id, null_probability=0.0),
        "category_id":        maybe_null(category_id, null_probability=0.0),
        "scroll_depth_pct":   random.randint(15, 100) if not session.is_bot else random.randint(0, 20),
        "duration_seconds":   random.randint(8, 240) if not session.is_bot else random.randint(0, 5),
        "viewport_width":     random.choice([375, 390, 414, 768, 1280, 1366, 1440, 1920]),
        "is_bot_suspected":   session.is_bot,
        # Null aléatoires pour simuler les adblockers qui bloquent certaines données
        "utm_source":         maybe_null(session.referrer.split("_")[0], 0.25),
        "utm_medium":         maybe_null("organic" if "ads" not in session.referrer else "cpc", 0.30),
    }


def generate_cart_event(session: UserSession, product_id: str, action: str = "add") -> dict:
    """
    Génère un événement panier.
    
    CONTRAINTE CRITIQUE : Un produit peut être ajouté au panier SEULEMENT
    si il a été vu dans la session (viewed_products). Cette contrainte
    garantit la cohérence comportementale pour le feature engineering ML.
    """
    product = PRODUCT_MAP.get(product_id, {})
    price = round(random.uniform(*product.get("price_range", (10.0, 100.0))), 2)
    
    return {
        "event_id":           str(uuid.uuid4()),
        "event_type":         f"cart_{action}",
        "page":               "product_detail",
        "user_id":            session.user_id,
        "session_id":         session.session_id,
        "timestamp":          session.get_current_timestamp(),
        "device_type":        session.device,
        "user_agent":         session.ua,
        "device_fingerprint": session.device_fingerprint,
        "ip_address":         session.ip,
        "product_id":         product_id,
        "category_id":        product.get("category_id"),
        "product_name":       product.get("name"),
        "product_price":      price,
        "quantity":           random.choices([1, 2, 3], weights=[0.80, 0.15, 0.05], k=1)[0],
        "is_adult_product":   product.get("adult_only", False),
        "referrer":           "internal",
        "scroll_depth_pct":   None,
        "duration_seconds":   random.randint(30, 300),
        "is_bot_suspected":   session.is_bot,
        "utm_source":         None,
        "utm_medium":         None,
    }


def generate_special_event(session: UserSession, event_type: str,
                           product_id: Optional[str] = None,
                           extra_data: Optional[dict] = None) -> dict:
    """
    Génère des événements spéciaux (KYC, fraude, marketing).
    
    CAS D'USAGE KYC :
      - "id_verification_started" : L'utilisateur a uploadé son CNI
        → corrélé avec id_document_path dans le CRM → input pour le modèle OCR
      - "age_gate_shown" : Affichage du gate "18+" avant un produit adulte
        → Feature importante : les mineurs voient souvent ce gate PLUSIEURS fois
      - "adult_content_blocked" : Blocage effectif
        → Ground truth pour évaluer l'efficacité du pipeline KYC
    """
    base = {
        "event_id":           str(uuid.uuid4()),
        "event_type":         event_type,
        "page":               "user_kyc" if "id_verif" in event_type else session.current_page,
        "user_id":            session.user_id,
        "session_id":         session.session_id,
        "timestamp":          session.get_current_timestamp(),
        "device_type":        session.device,
        "user_agent":         session.ua,
        "device_fingerprint": session.device_fingerprint,
        "ip_address":         session.ip,
        "product_id":         product_id,
        "category_id":        None,
        "referrer":           "internal",
        "scroll_depth_pct":   None,
        "duration_seconds":   None,
        "is_bot_suspected":   session.is_bot,
        "utm_source":         None,
        "utm_medium":         None,
    }
    if extra_data:
        base.update(extra_data)
    return base


# =============================================================================
# SIMULATION DE PARCOURS CLIENT
# =============================================================================

def simulate_session(user_id: str) -> list[dict]:
    """
    Simule un parcours complet d'une session utilisateur.
    Retourne la liste ordonnée des événements de la session.
    
    Parcours typiques modélisés :
      1. Browse & Leave (60%)  : navigation sans achat
      2. Add to Cart (25%)     : panier rempli mais abandon
      3. Full Conversion (15%) : commande complétée
      
    Cas d'usage FRAUDE et KYC intégrés dans les parcours.
    """
    session = UserSession(user_id)
    events = []
    user = session.user
    
    # ── Événement 1 : Arrivée sur le site ────────────────────────────────────
    events.append(generate_page_event(session, "homepage"))
    session.events_count += 1
    session.current_page = "homepage"
    
    # ── Simulation du tunnel de navigation ───────────────────────────────────
    visited_category = None
    current_product = None
    
    while session.events_count < session.max_events:
        
        # Navigation : choix de la prochaine page selon la page actuelle
        page_info = SITE_PAGES.get(session.current_page, SITE_PAGES["homepage"])
        next_page = random.choices(
            page_info["next_likely"],
            weights=page_info["weights"],
            k=1
        )[0]
        
        # ── PAGE CATÉGORIE ────────────────────────────────────────────────────
        if next_page == "category_listing":
            # Sélection d'une catégorie
            # Cas spécial : les jeunes adultes et adultes peuvent accéder à CAT-ADULT
            if user["is_minor"] or user["kyc_status"] in ("none", "pending"):
                # Mineurs et non-vérifiés naviguent sur les catégories standard
                cat_id = random.choice([c for c in PRODUCT_CATALOG if c != "CAT-ADULT"])
            else:
                # Adultes vérifiés : 10% de chance de naviguer sur CAT-ADULT
                all_cats = list(PRODUCT_CATALOG.keys())
                cat_id = random.choices(
                    all_cats,
                    weights=[0.1 if c == "CAT-ADULT" else 1.0 for c in all_cats],
                    k=1
                )[0]
            
            # Cas critique : mineur qui ESSAIE d'accéder à CAT-ADULT
            # CAS D'USAGE : Feature d'entraînement pour détection fraude KYC
            if cat_id == "CAT-ADULT" and (user["is_minor"] or user["kyc_status"] != "verified"):
                events.append(generate_special_event(
                    session, "age_gate_shown",
                    extra_data={"blocked_category": "CAT-ADULT", "user_age": user["age"]}
                ))
                session.events_count += 1
                
                if user["is_minor"]:
                    # Le mineur est détecté → blocage
                    events.append(generate_special_event(
                        session, "adult_content_blocked",
                        extra_data={
                            "blocked_category": "CAT-ADULT",
                            "user_age":         user["age"],
                            "kyc_status":       user["kyc_status"],
                            "block_reason":     "age_verification_failed"
                        }
                    ))
                    session.events_count += 1
                    # Redirigé vers la homepage après blocage
                    next_page = "homepage"
                    cat_id = None
                elif user["kyc_status"] in ("none", "pending"):
                    # Redirection vers la vérification KYC
                    events.append(generate_special_event(
                        session, "adult_content_blocked",
                        extra_data={
                            "blocked_category": "CAT-ADULT",
                            "user_age":         user["age"],
                            "kyc_status":       user["kyc_status"],
                            "block_reason":     "identity_verification_required"
                        }
                    ))
                    session.events_count += 1
                    # Redirigé vers la page KYC
                    next_page = "user_kyc"
                    cat_id = None
            
            if cat_id:
                visited_category = cat_id
                events.append(generate_page_event(
                    session, "category_listing", category_id=cat_id
                ))
        
        # ── PAGE KYC ─────────────────────────────────────────────────────────
        elif next_page == "user_kyc":
            events.append(generate_page_event(session, "user_kyc"))
            session.current_page = "user_kyc"
            session.events_count += 1
            
            # Simulation de la soumission de la CNI
            if random.random() < 0.55:  # 55% tentent la vérification
                events.append(generate_special_event(
                    session, "id_verification_started",
                    extra_data={
                        "document_type":     random.choice(["CNI", "passeport", "titre_sejour"]),
                        "upload_method":     random.choice(["camera", "file_upload"]),
                        "kyc_current_status": user["kyc_status"],
                    }
                ))
                session.events_count += 1
                
                # Résultat de la vérification
                if user["is_minor"] or user["kyc_status"] == "rejected":
                    # Échec : mineur détecté par OCR ou document rejeté
                    events.append(generate_special_event(
                        session, "id_verification_failed",
                        extra_data={
                            "failure_reason": "minor_detected" if user["is_minor"] else "document_rejected",
                            "user_age":       user["age"],
                            "document_path":  user["id_document_path"],
                        }
                    ))
                elif user["kyc_status"] == "verified":
                    events.append(generate_special_event(
                        session, "id_verification_success",
                        extra_data={"document_path": user["id_document_path"]}
                    ))
            
            next_page = "user_profile"  # Retour au profil après KYC
        
        # ── PAGE PRODUIT ──────────────────────────────────────────────────────
        elif next_page == "product_detail":
            # Sélection d'un produit dans la catégorie visitée
            if visited_category and random.random() < 0.75:
                cat_products = PRODUCT_CATALOG[visited_category]["products"]
                product = random.choice(cat_products)
            else:
                # Produit aléatoire (venu d'une recherche ou recommandation)
                product = random.choice(STANDARD_PRODUCTS)
            
            current_product = product["id"]
            # IMPORTANT : marquer comme vu AVANT de permettre l'ajout panier
            session.viewed_products.append(current_product)
            
            events.append(generate_page_event(
                session, "product_detail",
                product_id=current_product,
                category_id=product["category_id"]
            ))
        
        # ── AJOUT AU PANIER ───────────────────────────────────────────────────
        elif next_page == "cart_add":
            # CONTRAINTE MÉTIER : Seulement si le produit a été vu
            if session.viewed_products:
                # Choisir parmi les produits vus (pas forcément le dernier)
                product_to_cart = random.choice(session.viewed_products)
                product_info = PRODUCT_MAP.get(product_to_cart, {})
                
                # Vérification adulte pour les produits réservés +18
                if product_info.get("adult_only") and (
                    user["is_minor"] or user["kyc_status"] != "verified"
                ):
                    # Tentative bloquée → feature fraude importante
                    events.append(generate_special_event(
                        session, "adult_content_blocked",
                        product_id=product_to_cart,
                        extra_data={
                            "product_name":  product_info.get("name"),
                            "user_age":      user["age"],
                            "kyc_status":    user["kyc_status"],
                            "block_reason":  "age_verification_required_for_product",
                            "attempted_category": product_info.get("category_id"),
                        }
                    ))
                else:
                    events.append(generate_cart_event(session, product_to_cart, "add"))
                    session.carted_products.append(product_to_cart)
                    next_page = "cart_view"
            else:
                next_page = "homepage"  # Redirection si rien vu
        
        # ── VUE PANIER ────────────────────────────────────────────────────────
        elif next_page == "cart_view":
            if session.carted_products:
                events.append(generate_page_event(session, "cart_view"))
                # Abandon panier (70% réaliste)
                if random.random() < 0.70 and not session.completed_order:
                    next_page = random.choice(["homepage", "product_detail"])
            else:
                next_page = "homepage"
        
        # ── CHECKOUT ─────────────────────────────────────────────────────────
        elif next_page == "checkout_start":
            if session.carted_products:
                events.append(generate_page_event(session, "checkout_start"))
        
        elif next_page == "checkout_payment":
            if session.carted_products:
                events.append(generate_page_event(session, "checkout_payment"))
        
        elif next_page == "order_confirmation":
            if session.carted_products:
                events.append(generate_page_event(session, "order_confirmation"))
                session.completed_order = True
                # Arrêter la session après conversion
                break
        
        # ── PAGES GÉNÉRIQUES ─────────────────────────────────────────────────
        elif next_page in ("homepage", "search_results", "user_profile", "user_orders"):
            events.append(generate_page_event(session, next_page))
        
        session.current_page = next_page if next_page in SITE_PAGES else "homepage"
        session.events_count += 1
    
    return events


# =============================================================================
# PRODUCTEUR PRINCIPAL
# =============================================================================

def delivery_callback(err, msg):
    if err is not None:
        print(f"[ERROR] Clickstream delivery failed: {err}")


def run_producer(
    events_per_second: int = 25,
    duration_seconds:  int = 600,
    verbose:           bool = True
):
    """
    Lance le producteur clickstream.
    
    events_per_second = 25 en dev (pic soir ~500/s en prod)
    
    CAS D'USAGE VOLUME :
      - Heure de pointe (20h-21h) : ~300-500 events/s
      - Nuit (02h-04h) : ~5-10 events/s
      - Moyenne journalière : ~120 events/s
      Ce producteur simule un mélange de ces périodes.
    """
    # Pool d'utilisateurs actifs (loi de Pareto : 20% génèrent 80% du trafic)
    # CAS D'USAGE : Réalisme pour les features de fréquence du modèle ML
    vip_users    = [uid for uid, u in USER_POOL.items() if u["segment"] == "vip"]
    gold_users   = [uid for uid, u in USER_POOL.items() if u["segment"] == "gold"]
    silver_users = [uid for uid, u in USER_POOL.items() if u["segment"] == "silver"]
    bronze_users = [uid for uid, u in USER_POOL.items() if u["segment"] == "bronze"]
    
    # Les VIP et Gold génèrent beaucoup plus de sessions
    weighted_pool = (
        vip_users * 8 +
        gold_users * 4 +
        silver_users * 2 +
        bronze_users * 1 +
        MINOR_USER_IDS * 1  # Mineurs présents (cas d'usage KYC)
    )
    
    interval = 1.0 / events_per_second
    start_time = time.time()
    total_events = 0
    total_sessions = 0
    kyc_events = 0
    adult_block_events = 0
    
    print(f"[INFO] Démarrage producteur clickstream | {events_per_second} events/s")
    print(f"[INFO] Pool: {len(weighted_pool)} slots | Durée: {duration_seconds}s")
    print(f"[INFO] Utilisateurs mineurs dans le pool: {len(MINOR_USER_IDS)}")
    
    while time.time() - start_time < duration_seconds:
        # Générer une session complète pour un utilisateur
        user_id = random.choice(weighted_pool)
        session_events = simulate_session(user_id)
        
        for event in session_events:
            # Clé Kafka = user_id : garantit l'ordre des événements par utilisateur
            # Critique pour le feature engineering (séquences temporelles)
            producer.produce(
                topic=TOPIC,
                key=event["user_id"],
                value=json.dumps(event, ensure_ascii=False, default=str),
                callback=delivery_callback
            )
            
            # Compteurs pour le monitoring
            total_events += 1
            if event["event_type"] in ("id_verification_started", "id_verification_success", "id_verification_failed"):
                kyc_events += 1
            if event["event_type"] == "adult_content_blocked":
                adult_block_events += 1
            
            if total_events % 100 == 0:
                producer.poll(0)
            
            time.sleep(interval)
        
        total_sessions += 1
        
        if verbose and total_sessions % 50 == 0:
            elapsed = time.time() - start_time
            print(
                f"[INFO] Sessions: {total_sessions:,} | "
                f"Events: {total_events:,} | "
                f"KYC events: {kyc_events} | "
                f"Adult blocks: {adult_block_events} | "
                f"Elapsed: {elapsed:.0f}s"
            )
    
    producer.flush(timeout=30)
    
    print(f"\n[OK] Producteur clickstream terminé")
    print(f"  Sessions simulées   : {total_sessions:,}")
    print(f"  Événements envoyés  : {total_events:,}")
    print(f"  Événements KYC      : {kyc_events}")
    print(f"  Blocages adultes    : {adult_block_events}")
    print(f"  Taux KYC            : {kyc_events/max(total_events,1)*100:.1f}%")


if __name__ == "__main__":
    run_producer(
        events_per_second=20,
        duration_seconds=600,
        verbose=True
    )