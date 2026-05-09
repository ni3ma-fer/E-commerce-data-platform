# Power BI — Guide de connexion et tableaux de bord KiVendTout

## 1. Connexion DirectQuery sur ADLS Gen2 (Gold Delta)

### Prérequis
- Power BI Desktop ≥ mars 2024 (connecteur Delta Lake natif)
- Azure Data Lake Storage Gen2 activé sur le compte de stockage
- Token SAS ou Service Principal avec rôle **Storage Blob Data Reader** sur le container `medallion-data`

### Connexion via Azure Data Lake Storage Gen2

1. **Obtenir les données** → **Plus…** → rechercher **Azure Data Lake Storage Gen2**
2. URL du container :
   ```
   https://<AZURE_STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/medallion-data/gold/
   ```
3. Mode de connectivité : **DirectQuery** (évite l'import; requêtes en temps réel)
4. Authentification : choisir **Compte organisationnel** (SSO Entra ID) ou **Clé de compte**

### Connexion locale via DuckDB (dev_local)

Pour le développement sans ADLS, utiliser le connecteur ODBC DuckDB :

1. Installer [DuckDB ODBC Driver](https://duckdb.org/docs/api/odbc/overview)
2. Dans Power BI : **Obtenir les données** → **ODBC** → DSN `DuckDB`
3. Requête source :
   ```sql
   SELECT * FROM read_parquet('D:/Projet bloc 1/kivendtout-data-platform/dbt/kivendtout_local.duckdb/gold/**/*.parquet')
   ```

---

## 2. Modèle de données — Star Schema Kimball

```
         dim_date ─────────────────────────────────┐
         dim_customers ──────────────────────────┐  │
         dim_products ───────────────────────┐   │  │
         dim_merchants ──────────────────┐   │   │  │
                                         │   │   │  │
                                    fact_orders  │  │
                                         │       │  │
                                    fact_fraud_scores
                                         │       │  │
                                    kpi_daily ───┘  │
                                                    │
                                         └──────────┘
```

### Tables Gold disponibles

| Table | Granularité | Lignes estimées | Clés |
|---|---|---|---|
| `fact_orders` | 1 ligne / commande | ~500K/mois | order_id, customer_id, product_id, merchant_id, date_id |
| `fact_fraud_scores` | 1 ligne / transaction scorée | ~500K/mois | transaction_id, customer_id, model_version, scored_at |
| `dim_customers` | 1 ligne / client | ~50K | customer_id (PK) |
| `dim_products` | 1 ligne / produit | ~5K | product_id (PK) |
| `dim_merchants` | 1 ligne / marchand | ~2K | merchant_id (PK) |
| `dim_date` | 1 ligne / jour | 3650 | date_id (PK) |
| `kpi_daily` | 1 ligne / jour | ~365/an | date_id |
| `fraud_feature_store` | 1 ligne / transaction ML | ~500K/mois | transaction_id, user_id |

### Relations à créer dans Power BI (vue Modèle)

```
fact_orders[date_id]        → dim_date[date_id]         (M:1)
fact_orders[customer_id]    → dim_customers[customer_id] (M:1)
fact_orders[product_id]     → dim_products[product_id]   (M:1)
fact_orders[merchant_id]    → dim_merchants[merchant_id]  (M:1)
fact_fraud_scores[customer_id] → dim_customers[customer_id] (M:1)
fact_fraud_scores[date_id]     → dim_date[date_id]          (M:1)
kpi_daily[date_id]             → dim_date[date_id]          (M:1)
```

---

## 3. Optimisations DirectQuery

### Paramètres Power BI (Fichier → Options → DirectQuery)

```
Activer le mode DirectQuery illimité : ✓
Délai d'expiration de la connexion : 300 secondes
Nombre maximal de connexions par source de données : 10
```

### Agrégations (réduire les requêtes lourdes)

Pour `fact_orders` et `fact_fraud_scores`, créer une table d'agrégation :

1. Dans Power BI : clic droit sur `fact_orders` → **Gérer les agrégations**
2. Mapper `kpi_daily` comme agrégation de `fact_orders` sur `date_id`
3. Les visuels au niveau journalier utiliseront automatiquement `kpi_daily`

### Filtres de date obligatoires (éviter full scans)

Ajouter un **filtre de rapport** sur `dim_date[year]` ou utiliser un slicer de plage de dates. Sans filtre, DirectQuery scanne toutes les partitions Delta.

---

## 4. Tableau de bord Commercial

**Objectif :** Suivi des ventes, panier moyen, top marchands.

### Visuels recommandés

| Visual | Champs | Description |
|---|---|---|
| Carte KPI | `SUM(fact_orders[amount])` | CA total période |
| Carte KPI | `DISTINCTCOUNT(fact_orders[customer_id])` | Clients actifs |
| Courbe temporelle | `dim_date[date]`, `SUM(fact_orders[amount])` | Évolution CA |
| Histogramme | `dim_merchants[merchant_name]`, `SUM(fact_orders[amount])` | Top 10 marchands |
| Treemap | `dim_products[category]`, `COUNT(fact_orders[order_id])` | Volume par catégorie |
| Tableau | `dim_customers`, `COUNT`, `SUM`, `AVG amount` | Clients VIP |

### Mesures DAX

```dax
CA Total = SUM(fact_orders[amount])

Panier Moyen = DIVIDE([CA Total], COUNTROWS(fact_orders))

Croissance MoM % =
VAR curr = [CA Total]
VAR prev = CALCULATE([CA Total], DATEADD(dim_date[date], -1, MONTH))
RETURN DIVIDE(curr - prev, prev)

Clients Actifs = DISTINCTCOUNT(fact_orders[customer_id])

Taux Retention =
DIVIDE(
    CALCULATE(DISTINCTCOUNT(fact_orders[customer_id]),
              FILTER(fact_orders, fact_orders[order_count_30d] > 1)),
    [Clients Actifs]
)
```

---

## 5. Tableau de bord Logistique

**Objectif :** Délais de livraison, taux de retour, couverture géographique.

### Visuels recommandés

| Visual | Champs | Description |
|---|---|---|
| Carte géographique | `dim_customers[country]`, `COUNT(fact_orders)` | Volume par pays |
| Jauge | `AVG(fact_orders[delivery_days])` | Délai moyen livraison |
| Courbe | `dim_date[date]`, `COUNT(fact_orders[is_returned])` | Retours dans le temps |
| Matrice | `dim_merchants[region]`, `dim_date[month_name]`, `AVG(delivery_days)` | Heat map délais |
| Entonnoir | Étapes commande (placed → shipped → delivered → returned) | Funnel logistique |

### Mesures DAX

```dax
Délai Moyen (j) = AVERAGE(fact_orders[delivery_days])

Taux Retour % = DIVIDE(
    COUNTROWS(FILTER(fact_orders, fact_orders[is_returned] = TRUE())),
    COUNTROWS(fact_orders)
) * 100

Commandes En Retard =
COUNTROWS(FILTER(fact_orders, fact_orders[delivery_days] > 5))

Taux Livraison J+3 =
DIVIDE(
    COUNTROWS(FILTER(fact_orders, fact_orders[delivery_days] <= 3)),
    COUNTROWS(fact_orders)
) * 100
```

---

## 6. Tableau de bord Fraude

**Objectif :** Monitoring des scores XGBoost, alertes, patterns de fraude.

### Visuels recommandés

| Visual | Champs | Description |
|---|---|---|
| Carte KPI (rouge) | `COUNTROWS(FILTER(fact_fraud_scores, [fraud_decision] = "BLOCK"))` | Transactions bloquées |
| Carte KPI | `AVERAGE(fact_fraud_scores[fraud_probability])` | Score moyen |
| Courbe | `dim_date[date]`, `COUNTROWS BLOCK` | Tendance fraude |
| Histogramme | Distribution de `fraud_probability` (bins 0.1) | Distribution scores |
| Scatter plot | `geo_distance_km` vs `fraud_probability` | Geo vs risque |
| Tableau | Transactions BLOCK avec customer_id, amount, country | File de review |
| Donut | Répartition BLOCK / 3DS_REQUIRED / APPROVED | Décisions |

### Mesures DAX

```dax
Transactions Bloquées =
COUNTROWS(FILTER(fact_fraud_scores, fact_fraud_scores[fraud_decision] = "BLOCK"))

Score Fraude Moyen = AVERAGE(fact_fraud_scores[fraud_probability])

Taux Fraude % =
DIVIDE([Transactions Bloquées], COUNTROWS(fact_fraud_scores)) * 100

Faux Positifs Estimés =
COUNTROWS(FILTER(fact_fraud_scores,
    fact_fraud_scores[fraud_decision] = "BLOCK" &&
    fact_fraud_scores[confirmed_fraud] = FALSE()
))

Économies Estimées (€) =
SUMX(
    FILTER(fact_fraud_scores, fact_fraud_scores[fraud_decision] = "BLOCK"),
    fact_fraud_scores[transaction_amount]
)
```

### Alertes Power BI (Données → Alertes)

Sur la carte KPI "Transactions Bloquées" :
- Seuil : `> 50` transactions bloquées en 1 heure
- Notification : e-mail + notification Power BI mobile

---

## 7. Alertes Slack temps réel (déjà implémentées)

L'API déclenche automatiquement une alerte Slack quand `fraud_probability ≥ 0.80` :

```python
# api/core/alerts.py
FRAUD_THRESHOLD = float(os.getenv("FRAUD_ALERT_THRESHOLD", "0.80"))
```

### Configuration

Dans `.env` :
```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../...
FRAUD_ALERT_THRESHOLD=0.80
```

### Format du message Slack

```
🚨 [FRAUD ALERT] Transaction TXN-XXX bloquée
Score: 0.923 | Décision: BLOCK
Utilisateur: analyst | Heure: 2026-05-09T18:30:00Z
```

---

## 8. Commandes de démarrage rapide

```bash
# Démarrer Redis + API
docker compose -f docker/docker-compose.api.yml up -d

# Démarrer l'API en développement (avec rechargement auto)
cd '/mnt/d/Projet bloc 1/kivendtout-data-platform'
source .venv/bin/activate
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# Tester l'API
curl -X POST http://localhost:8000/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}'

# Documentation interactive
# → http://localhost:8000/docs
# → http://localhost:8000/redoc
```
