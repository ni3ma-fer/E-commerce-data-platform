# KiVendTout Data Platform

Pipeline de données complet pour une marketplace e-commerce — architecture Medallion (Bronze → Silver → Gold), détection de fraude XGBoost, OCR KYC, API REST et tableaux de bord Power BI.

---

## Architecture

```
Producteurs Kafka          Couche Bronze          Couche Silver         Couche Gold
─────────────────          ─────────────          ─────────────         ───────────
payment_producer.py  ──►  Spark Streaming  ──►   dbt models      ──►  Star Schema
clickstream_producer ──►  → ADLS Gen2           silver_payments       fact_orders
logistics_producer   ──►  (Parquet Delta)        silver_crm            fact_fraud_scores
                                                 silver_clickstream    dim_customers
                          Orchestration           silver_sessions       dim_products
                          ─────────────           silver_ip_rep.        dim_merchants
                          Airflow 2.9             (Presidio PII)        kpi_daily
                          5 DAGs                                        fraud_feature_store
                          02h–05h UTC
                                                ML Pipeline
                                                ──────────────
                                                XGBoost Fraud          API REST
                                                (MLflow registry) ──►  ──────────
                                                OCR Tesseract          FastAPI 0.111
                                                (CNI → KYC)            Redis cache
                                                                       JWT auth
                                                                       Swagger UI
                                                                       :8000/docs
```

---

## Stack technique

| Couche | Technologie | Version |
|--------|-------------|---------|
| Ingestion | Apache Kafka (Confluent) | 7.6.0 |
| Processing | Apache Spark Streaming | 3.x |
| Orchestration | Apache Airflow | 2.9.1 |
| Transformation | dbt-core + dbt-duckdb | 1.x |
| Stockage Cloud | Azure ADLS Gen2 | — |
| Stockage Local | DuckDB | 0.10.3 |
| ML Fraud | XGBoost + scikit-learn | 2.0.3 |
| ML OCR | Tesseract 5.3.4 + OpenCV | — |
| Tracking ML | MLflow | 2.13.0 |
| API | FastAPI + Uvicorn | 0.111.0 |
| Cache | Redis | 7-alpine |
| Auth | JWT (python-jose) | — |
| Conteneurs | Docker Compose | — |

---

## Structure du projet

```
kivendtout-data-platform/
├── airflow/
│   └── dags/
│       ├── dag_bronze_ingestion.py   # CRM → ADLS Bronze (02h UTC)
│       ├── dag_silver_transform.py   # Spark + Presidio + dbt (04h UTC)
│       ├── dag_gold_build.py         # dbt Star Schema (05h UTC)
│       ├── dag_ml_retrain.py         # Retrain XGBoost (dim. 03h UTC)
│       └── dag_ml_ocr.py             # OCR CNI → KYC (horaire)
│
├── api/
│   ├── main.py                       # FastAPI app, JWT, rate limiting
│   ├── Dockerfile
│   ├── core/
│   │   ├── auth.py                   # JWT Bearer
│   │   ├── cache.py                  # Redis (lazy init)
│   │   └── alerts.py                 # Slack webhooks
│   └── routers/
│       ├── fraud.py                  # GET /fraud-score/{txn_id}
│       ├── ocr.py                    # POST /ocr/verify-id
│       ├── kyc.py                    # GET /user/{id}/kyc-status
│       └── gdpr.py                   # DELETE /gdpr/erase/{id}
│
├── dbt/
│   └── models/
│       ├── silver/                   # 5 modèles (paiements, CRM, sessions…)
│       └── gold/                     # 8 modèles (facts, dims, KPIs, features)
│
├── docker/
│   ├── docker-compose.kafka.yml      # Kafka + Zookeeper + UI
│   ├── docker-compose.airflow.yml    # Airflow + Postgres
│   └── docker-compose.api.yml        # FastAPI + Redis
│
├── ingestion/
│   ├── payment_producer.py           # Topic payments-raw (6 partitions)
│   ├── clickstream_producer.py       # Topic clickstream-raw (6 partitions)
│   └── Logistics_producer.py         # Topic logistics-raw (3 partitions)
│
├── ml/
│   ├── fraud_detection/
│   │   ├── train.py                  # Entraînement XGBoost + MLflow
│   │   └── predict.py                # Scoring streaming Spark
│   └── ocr_pipeline/
│       ├── preprocess.py             # CLAHE + débruitage OpenCV
│       └── extract.py                # Regex CNI → nom/prénom/âge/décision KYC
│
├── spark/
│   └── bronze/
│       └── kafka_to_bronze.py        # Spark Streaming → ADLS Delta
│
├── docs/
│   └── powerbi_guide.md              # Connexion Power BI + DAX
│
├── requirements-api.txt
├── requirements-ml.txt
└── Makefile
```

---

## Démarrage rapide

### Prérequis

- Docker Desktop (WSL2 backend)
- Python 3.12 dans WSL Ubuntu
- Tesseract OCR (`apt install tesseract-ocr tesseract-ocr-fra`)
- Compte Azure Storage (optionnel — fallback synthétique disponible)

### 1. Variables d'environnement

```bash
cp .env.example .env   # puis remplir les valeurs
```

Variables requises :

```env
AZURE_STORAGE_ACCOUNT_NAME=...
AZURE_STORAGE_ACCOUNT_KEY=...
AZURE_CONTAINER_NAME=medallion-data
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
MLFLOW_TRACKING_URI=http://localhost:5000
JWT_SECRET_KEY=change-in-production
```

### 2. Infrastructure (Kafka + Airflow)

```bash
# Kafka + Zookeeper + UI (port 8090)
docker compose -f docker/docker-compose.kafka.yml up -d

# Airflow (port 8080 — admin/admin)
docker compose -f docker/docker-compose.airflow.yml up -d
```

### 3. MLflow + entraînement du modèle

```bash
# Démarrer MLflow (port 5000)
source .venv/bin/activate
mlflow server --host 0.0.0.0 --port 5000 &

# Entraîner le modèle XGBoost (enregistre dans MLflow)
python ml/fraud_detection/train.py
```

Le modèle est enregistré sous `fraud-xgboost-champion` → stage **Production**.
Fallback synthétique automatique si ADLS est indisponible.

### 4. API REST

```bash
# Redis + API (ports 6379 + 8000)
docker compose -f docker/docker-compose.api.yml up -d

# OU en mode développement (rechargement auto)
source .venv/bin/activate
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

**Swagger UI :** http://localhost:8000/docs

---

## Endpoints API

| Méthode | Endpoint | Auth | Description |
|---------|----------|------|-------------|
| `POST` | `/auth/token` | — | Obtenir un JWT Bearer |
| `GET` | `/fraud-score/{txn_id}` | Bearer | Score fraude (cache Redis → XGBoost) |
| `POST` | `/fraud-score/realtime` | Bearer | Scoring temps réel avec features brutes |
| `POST` | `/ocr/verify-id` | Bearer | Pipeline OCR CNI → décision KYC |
| `GET` | `/user/{id}/kyc-status` | Bearer | Statut KYC d'un utilisateur |
| `DELETE` | `/gdpr/erase/{id}` | Bearer (admin) | Droit à l'effacement (RGPD Art. 17) |
| `GET` | `/health` | — | État Redis + MLflow |

**Comptes de démonstration :** `admin/admin` · `analyst/analyst`

### Exemple — Score fraude

```bash
# 1. Authentification
TOKEN=$(curl -s -X POST http://localhost:8000/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# 2. Score fraude
curl -s http://localhost:8000/fraud-score/TXN-001 \
  -H "Authorization: Bearer $TOKEN"
```

```json
{
  "transaction_id": "TXN-001",
  "fraud_probability": 0.478763,
  "fraud_decision": "APPROVED",
  "model_version": "fraud-xgboost-champion/Production",
  "cached": false
}
```

Décisions possibles : `APPROVED` (< 0.50) · `3DS_REQUIRED` (0.50–0.80) · `BLOCK` (≥ 0.80)

---

## Modèle ML Fraude

**Algorithme :** XGBoost Classifier (Champion/Challenger via MLflow)

**20 features :**

| Catégorie | Features |
|-----------|----------|
| Vélocité (7) | txn_count_5min, txn_count_1h, txn_count_30d, avg_amount_30d, amount_vs_avg_ratio, amount_stddev_30d, geo_distance_km |
| Contexte (7) | kyc_status, age_group, risk_category, category_id, payment_method, billing_country_iso, shipping_country_iso |
| Binaires (6) | is_cross_border, user_is_minor, is_first_transaction, is_new_card, is_new_merchant, ip_geo_mismatch |

**6 patterns de fraude détectés :** P1 adulte/mineur · P2 KYC bypass · P3 attaque vélocité · P4 géo mismatch · P5 card testing · P6 prise de contrôle de compte

---

## Pipeline OCR KYC

```
Image CNI (JPEG/PNG)
       │
       ▼
preprocess_for_ocr()     ← CLAHE, débruitage, resize 300 DPI
       │
       ▼
extract_cni_data()       ← Tesseract 5 (fra+eng), regex nom/prénom/naissance
       │
       ▼
Décision KYC             ← APPROVED / MANUAL_REVIEW / REJECTED_MINOR
       │
       ▼
Cache Redis (user_id)    ← Accessible via GET /user/{id}/kyc-status
```

Le numéro de carte est hashé SHA-256 immédiatement (RGPD Art. 5).

---

## Conformité RGPD

| Article | Implémentation |
|---------|---------------|
| Art. 5 — Minimisation | Hash SHA-256 immédiat des données CNI |
| Art. 17 — Effacement | `DELETE /gdpr/erase/{id}` → purge Redis + pseudonymisation Silver/Gold |
| Art. 25 — Privacy by design | Presidio PII anonymization dans le pipeline Silver |
| Mineurs | Blocage automatique (is_minor=True → kyc_decision=REJECTED_MINOR) |

---

## DAGs Airflow

| DAG | Schedule | SLA |
|-----|----------|-----|
| `bronze_crm_ingestion` | `0 2 * * *` | < 30 min |
| `silver_daily_transform` | `0 4 * * *` | < 45 min |
| `gold_daily_build` | `0 5 * * *` | < 30 min |
| `ml_ocr_cni_processing` | `0 * * * *` | < 15 min |
| `ml_fraud_weekly_retrain` | `0 3 * * 0` | < 60 min |

---

## Power BI

Guide complet de connexion DirectQuery, modèle Star Schema Kimball, mesures DAX et 3 tableaux de bord (Commercial, Logistique, Fraude) : [docs/powerbi_guide.md](docs/powerbi_guide.md)

---

## Alertes temps réel

L'API envoie automatiquement une notification Slack quand `fraud_probability ≥ 0.80` :

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
FRAUD_ALERT_THRESHOLD=0.80
```

---

## Interfaces web

| Service | URL | Identifiants |
|---------|-----|--------------|
| API Swagger | http://localhost:8000/docs | admin/admin |
| Airflow | http://localhost:8080 | admin/admin |
| Kafka UI | http://localhost:8090 | — |
| MLflow | http://localhost:5000 | — |

---

## Licence

Projet académique — Bloc 1 Data Engineering.
