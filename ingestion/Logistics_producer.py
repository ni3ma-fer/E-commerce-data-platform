"""
=============================================================================
logistics_producer.py  —  KiVendTout  |  Étape 2 : Ingestion Kafka
=============================================================================
Producteur Kafka pour les événements de logistique et de livraison.

CAS D'USAGE DIRECTION ADRESSÉS :
  1. ANALYSE OPÉRATIONNELLE :
     - KPIs de performance livraison par transporteur et par région
     - Taux de retard par zone géographique et par catégorie produit
     - Détection des entrepôts surchargés (stock_level critique)

  2. DÉTECTION FRAUDE (SECONDAIRE) :
     - Cohérence entre l'adresse de livraison et le billing_country du paiement
     - Les commandes liées à des fraudes P4 (geo_mismatch) ont des adresses
       de livraison suspectes → corrélation inter-topics dans Spark Silver

  3. ML PRÉDICTIF :
     - Prédiction des retards de livraison (features : transporteur, zone, poids)
     - Optimisation des stocks par entrepôt (séries temporelles)

ARCHITECTURE :
  Source : Python script (simule les webhooks des transporteurs)
  Sink   : Apache Kafka topic "logistics-raw" (3 partitions)
  Format : JSON UTF-8, clé = order_id
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
    USER_POOL, USER_IDS, PRODUCT_MAP, ALL_PRODUCTS,
    get_legit_ip, get_realistic_timestamp, maybe_null,
    FRENCH_CITIES, CARRIERS,
)

load_dotenv("../docker/.env")

# ─── CONFIGURATION KAFKA ──────────────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "client.id":         "logistics-producer-kivendtout-v2",
    "acks":              "all",
    "retries":           5,
    "compression.type":  "snappy",
}
TOPIC = "logistics-raw"

producer = Producer(KAFKA_CONFIG)

# ─── ENTREPÔTS ────────────────────────────────────────────────────────────────
WAREHOUSES = [
    {"id": "WH-PARIS-01",   "city": "Clichy",       "postal_code": "92110", "region": "IDF"},
    {"id": "WH-PARIS-02",   "city": "Bobigny",       "postal_code": "93000", "region": "IDF"},
    {"id": "WH-LYON-01",    "city": "Vénissieux",    "postal_code": "69200", "region": "ARA"},
    {"id": "WH-BORDEAUX-01","city": "Mérignac",      "postal_code": "33700", "region": "NAQ"},
    {"id": "WH-LILLE-01",   "city": "Villeneuve d'Ascq","postal_code":"59491","region":"HDF"},
    {"id": "WH-NANTES-01",  "city": "Saint-Herblain","postal_code": "44800", "region": "PDL"},
    {"id": "WH-MARSEILLE-01","city":"Vitrolles",     "postal_code": "13127", "region": "PAC"},
]

# Capacité et spécialisation par entrepôt
WAREHOUSE_SPECIALIZATION = {
    "WH-PARIS-01":    ["CAT-ELEC", "CAT-MODE"],
    "WH-PARIS-02":    ["CAT-MAISON", "CAT-SPORT"],
    "WH-LYON-01":     ["CAT-ELEC", "CAT-BEAUTE"],
    "WH-BORDEAUX-01": ["CAT-LIVRES", "CAT-ADULT"],  # Produits adultes sécurisés
    "WH-LILLE-01":    ["CAT-MODE", "CAT-SPORT"],
    "WH-NANTES-01":   ["CAT-MAISON", "CAT-BEAUTE"],
    "WH-MARSEILLE-01":["CAT-ELEC", "CAT-LIVRES"],
}

# Performances réelles des transporteurs (taux de retard sectoriels)
CARRIER_PERFORMANCE = {
    "chronopost":    {"delay_rate": 0.06, "avg_days": 1.0, "price_factor": 1.8},
    "colissimo":     {"delay_rate": 0.12, "avg_days": 2.0, "price_factor": 1.0},
    "ups":           {"delay_rate": 0.08, "avg_days": 1.5, "price_factor": 1.6},
    "dhl":           {"delay_rate": 0.07, "avg_days": 1.5, "price_factor": 1.7},
    "mondial_relay": {"delay_rate": 0.05, "avg_days": 3.0, "price_factor": 0.7},
    "fedex":         {"delay_rate": 0.05, "avg_days": 1.2, "price_factor": 2.0},
    "geodis":        {"delay_rate": 0.10, "avg_days": 2.5, "price_factor": 0.9},
}

# Statuts de livraison dans l'ordre logique du tunnel
ORDER_STATUS_FLOW = [
    "order_placed",
    "payment_confirmed",
    "warehouse_processing",
    "ready_for_pickup",
    "shipped",
    "in_transit",
    "out_for_delivery",
    "delivered",
]
# Statuts d'exception
EXCEPTION_STATUSES = ["delayed", "delivery_failed", "return_requested", "returned"]


def select_warehouse_for_product(product_id: str) -> dict:
    """Sélectionne l'entrepôt le plus adapté à la catégorie du produit"""
    product = PRODUCT_MAP.get(product_id, {})
    cat_id  = product.get("category_id", "")
    
    for wh in WAREHOUSES:
        if cat_id in WAREHOUSE_SPECIALIZATION.get(wh["id"], []):
            if random.random() < 0.75:  # 75% depuis l'entrepôt spécialisé
                return wh
    
    return random.choice(WAREHOUSES)  # Fallback


def generate_logistics_event(
    order_id: Optional[str] = None,
    user_id:  Optional[str] = None,
    status:   Optional[str] = None,
) -> dict:
    """
    Génère un événement logistique réaliste.
    
    COHÉRENCE RELATIONNELLE :
      - user_id est tiré du USER_POOL pour garantir la jointure avec CRM
      - product_id est tiré du PRODUCT_MAP pour la jointure avec le catalogue
      - Le poids du colis est cohérent avec la catégorie produit
    """
    if user_id is None:
        user_id = random.choice(USER_IDS)
    
    user = USER_POOL[user_id]
    
    if order_id is None:
        order_id = f"ORD-{random.randint(100000, 999999)}"
    
    # Sélection du produit
    product = random.choice(ALL_PRODUCTS)
    warehouse = select_warehouse_for_product(product["id"])
    
    # Sélection du transporteur (les gros achats vont chez des transporteurs premium)
    product_price_max = product["price_range"][1]
    if product_price_max > 200:
        carrier = random.choices(
            ["chronopost", "ups", "dhl", "fedex"],
            weights=[0.35, 0.30, 0.25, 0.10], k=1
        )[0]
    else:
        carrier = random.choices(
            list(CARRIER_PERFORMANCE.keys()),
            weights=[1.5, 2.0, 1.2, 1.2, 2.5, 0.8, 1.5], k=1
        )[0]
    
    carrier_perf = CARRIER_PERFORMANCE[carrier]
    is_delayed   = random.random() < carrier_perf["delay_rate"]
    
    if status is None:
        if is_delayed and random.random() < 0.4:
            status = random.choice(EXCEPTION_STATUSES[:2])  # delayed ou failed
        else:
            status = random.choice(ORDER_STATUS_FLOW)
    
    # Dates cohérentes avec le statut
    ts = get_realistic_timestamp()
    order_date = ts - timedelta(days=random.randint(0, 5))
    
    # Date de livraison estimée selon le transporteur
    estimated_days = carrier_perf["avg_days"]
    if is_delayed:
        estimated_days += random.uniform(1, 3)
    estimated_delivery = order_date + timedelta(days=estimated_days)
    
    actual_delivery = None
    if status == "delivered":
        deviation = random.uniform(-0.3, 1.5) if is_delayed else random.uniform(-0.2, 0.3)
        actual_delivery = (estimated_delivery + timedelta(days=deviation)).isoformat() + "Z"
    
    # Adresse de livraison (cohérente avec le pays de l'utilisateur)
    if user["country"] == "FR":
        city_info = random.choice(FRENCH_CITIES)
        delivery_city = city_info[0]
        delivery_postal = city_info[1]
        delivery_country = "FR"
    else:
        delivery_city = maybe_null(f"City-{user['country']}", 0.0)
        delivery_postal = f"{random.randint(1000, 99999):05d}"
        delivery_country = user["country"]
    
    # Poids du colis selon la catégorie
    category_weight_ranges = {
        "CAT-ELEC":   (0.2, 3.5),
        "CAT-MODE":   (0.1, 2.0),
        "CAT-MAISON": (0.5, 15.0),
        "CAT-SPORT":  (0.3, 12.0),
        "CAT-BEAUTE": (0.05, 0.8),
        "CAT-LIVRES": (0.1, 2.5),
        "CAT-ADULT":  (0.1, 1.5),
    }
    cat_id = product.get("category_id", "CAT-ELEC")
    weight_range = category_weight_ranges.get(cat_id, (0.1, 5.0))
    weight_kg = round(random.uniform(*weight_range), 2)
    
    return {
        "event_id":             str(uuid.uuid4()),
        "order_id":             order_id,
        "user_id":              user_id,
        "product_id":           product["id"],
        "product_category_id":  cat_id,
        "is_adult_product":     product["adult_only"],
        "status":               status,
        "previous_status":      maybe_null(
            ORDER_STATUS_FLOW[max(0, ORDER_STATUS_FLOW.index(status)-1)]
            if status in ORDER_STATUS_FLOW and ORDER_STATUS_FLOW.index(status) > 0
            else None,
            null_probability=0.15
        ),
        "carrier":              carrier,
        "tracking_number":      maybe_null(
            f"FR{random.randint(10**11, 10**12-1)}", null_probability=0.05
        ),
        "warehouse_id":         warehouse["id"],
        "warehouse_city":       warehouse["city"],
        "delivery_city":        delivery_city,
        "delivery_postal_code": delivery_postal,
        "delivery_country":     delivery_country,
        "order_date":           order_date.isoformat() + "Z",
        "estimated_delivery":   estimated_delivery.isoformat() + "Z",
        "actual_delivery":      actual_delivery,
        "timestamp":            ts.isoformat() + "Z",
        "weight_kg":            weight_kg,
        "nb_items":             random.choices([1, 2, 3, 4], weights=[0.65, 0.22, 0.09, 0.04], k=1)[0],
        "is_delayed":           is_delayed,
        "delay_reason":         maybe_null(
            random.choice([
                "transporteur_surchargé", "adresse_incorrecte",
                "absent_livraison", "intempéries", "grève_transport",
                "erreur_entrepôt", "problème_douane"
            ]) if is_delayed else None,
            null_probability=0.0
        ),
        "shipping_cost_eur":    round(weight_kg * carrier_perf["price_factor"] * random.uniform(0.8, 1.2), 2),
        # Feature pour corréler avec la fraude P4 (geo_mismatch)
        "billing_shipping_country_match": delivery_country == user["country"],
        # Niveau de stock de l'entrepôt source (feature pour la BI)
        "warehouse_stock_level": random.choices(
            ["critical", "low", "normal", "high"],
            weights=[0.05, 0.15, 0.60, 0.20], k=1
        )[0],
        "customer_rating":      maybe_null(
            random.choices([1, 2, 3, 4, 5], weights=[0.03, 0.05, 0.12, 0.30, 0.50], k=1)[0]
            if status == "delivered" else None,
            null_probability=0.40  # 40% ne notent pas
        ),
    }


def delivery_callback(err, msg):
    if err is not None:
        print(f"[ERROR] Logistics delivery failed: {err}")


def run_producer(duration_seconds: int = 600, events_per_minute: int = 40, verbose: bool = True):
    """
    Lance le producteur logistique.
    
    Taux plus faible que le clickstream : une commande génère ~5-8 événements
    de statut sur plusieurs jours. Ce producteur simule un sous-ensemble
    de ces événements avec des timestamps rétrospectifs réalistes.
    """
    interval = 60.0 / events_per_minute
    start_time = time.time()
    total_events = 0
    delayed_count = 0
    adult_product_count = 0
    
    print(f"[INFO] Démarrage producteur logistique | {events_per_minute} events/min")
    
    while time.time() - start_time < duration_seconds:
        # Générer une commande avec plusieurs événements de statut
        order_id = f"ORD-{random.randint(100000, 999999)}"
        user_id  = random.choice(USER_IDS)
        
        # Simuler plusieurs statuts pour la même commande (timeline logique)
        n_status_updates = random.choices([1, 2, 3, 4], weights=[0.35, 0.30, 0.20, 0.15], k=1)[0]
        max_status_index = random.randint(n_status_updates, len(ORDER_STATUS_FLOW)-1)
        statuses_to_emit = ORDER_STATUS_FLOW[:max_status_index+1][-n_status_updates:]
        
        for status in statuses_to_emit:
            event = generate_logistics_event(
                order_id=order_id,
                user_id=user_id,
                status=status
            )
            
            producer.produce(
                topic=TOPIC,
                key=order_id,
                value=json.dumps(event, ensure_ascii=False, default=str),
                callback=delivery_callback
            )
            
            total_events += 1
            if event["is_delayed"]:
                delayed_count += 1
            if event["is_adult_product"]:
                adult_product_count += 1
            
            if total_events % 30 == 0:
                producer.poll(0)
        
        if verbose and total_events % 200 == 0:
            elapsed = time.time() - start_time
            print(
                f"[INFO] Events: {total_events:,} | "
                f"Retards: {delayed_count} ({delayed_count/max(total_events,1)*100:.1f}%) | "
                f"Produits adultes: {adult_product_count} | "
                f"Elapsed: {elapsed:.0f}s"
            )
        
        time.sleep(interval)
    
    producer.flush(timeout=30)
    print(f"\n[OK] Producteur logistique terminé | {total_events:,} événements")


if __name__ == "__main__":
    run_producer(duration_seconds=600, events_per_minute=40, verbose=True)