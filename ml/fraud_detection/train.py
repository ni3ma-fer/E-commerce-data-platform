# ml/fraud_detection/train.py
"""
Entraînement XGBoost pour la détection de fraude.
 
SOURCE  : fraud_feature_store Gold (lu depuis Azure ADLS Gen2 via PyArrow)
TARGET  : is_fraud (binaire — 0 = légitime, 1 = frauduleux)
SORTIE  : modèle enregistré dans MLflow Model Registry (stage Production si F1 >= 0.90)
 
EXÉCUTION :
    source .venv/bin/activate
    python ml/fraud_detection/train.py
"""
import os, warnings
import numpy as np
import pandas as pd
import mlflow
import mlflow.xgboost
import xgboost as xgb
import shap
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import (f1_score, roc_auc_score,
    precision_score, recall_score, classification_report)
from sklearn.preprocessing import LabelEncoder
from dotenv import load_dotenv
warnings.filterwarnings("ignore")
load_dotenv()
 
# ── Configuration MLflow ─────────────────────────────────────────────────
MLFLOW_URI  = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT  = "kivendtout-fraud-detection"
MODEL_NAME  = "fraud-xgboost-champion"
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment(EXPERIMENT)
 
# ── Chargement depuis Azure ADLS Gold ────────────────────────────────────
def load_features_from_adls() -> pd.DataFrame:
    account   = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    key       = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
    container = os.getenv("AZURE_CONTAINER_NAME", "medallion-data")
    path      = f"abfs://{container}@{account}.dfs.core.windows.net/gold/fraud_feature_store"
    storage_options = {"account_name": account, "account_key": key}
    try:
        df = pd.read_parquet(path, storage_options=storage_options)
        print(f"Dataset chargé depuis ADLS : {len(df):,} lignes")
    except Exception as e:
        print(f"ADLS non disponible ({type(e).__name__}: {e})")
        print("→ Génération de données synthétiques locales (50 000 transactions)")
        df = _generate_synthetic_features(n=50_000, fraud_rate=0.05)
    print(f"Taux de fraude : {df['is_fraud'].mean()*100:.2f}%")
    return df


def _generate_synthetic_features(n: int = 50_000, fraud_rate: float = 0.05) -> pd.DataFrame:
    """Génère un feature store synthétique réaliste pour l'entraînement local."""
    _COUNTRIES = ["FR", "DE", "GB", "US", "CN", "NG", "RU"]
    _COUNTRY_P = [0.40, 0.15, 0.15, 0.15, 0.05, 0.05, 0.05]

    rng = np.random.default_rng(42)
    is_fraud = (rng.random(n) < fraud_rate).astype(int)
    country_sample = rng.choice(_COUNTRIES, n, p=_COUNTRY_P)

    df = pd.DataFrame({
        # Numériques — distributions différentes fraude vs légitime
        "txn_count_5min":      np.where(is_fraud, rng.integers(3, 20, n), rng.integers(1, 5, n)).astype(float),
        "txn_count_1h":        np.where(is_fraud, rng.integers(5, 40, n), rng.integers(1, 10, n)).astype(float),
        "txn_count_30d":       rng.integers(1, 200, n).astype(float),
        "avg_amount_30d":      rng.uniform(10, 500, n),
        "amount_vs_avg_ratio": np.where(is_fraud, rng.uniform(3, 15, n), rng.uniform(0.5, 2.5, n)),
        "amount_stddev_30d":   rng.uniform(5, 300, n),
        "geo_distance_km":     np.where(is_fraud, rng.uniform(500, 15000, n), rng.uniform(0, 200, n)),
        # Catégorielles
        "kyc_status":          rng.choice(["verified", "pending", "rejected", "unknown"], n,
                                           p=[0.70, 0.15, 0.08, 0.07]),
        "age_group":           rng.choice(["18-25", "26-35", "36-50", "51-65", "65+"], n),
        "risk_category":       rng.choice(["low", "medium", "high"], n, p=[0.65, 0.25, 0.10]),
        "category_id":         rng.integers(1, 50, n).astype(str),
        "payment_method":      rng.choice(["card", "bank_transfer", "wallet", "crypto"], n,
                                           p=[0.60, 0.20, 0.15, 0.05]),
        "billing_country_iso":  country_sample,
        "shipping_country_iso": country_sample,
        # Binaires
        "is_cross_border":      np.where(is_fraud, rng.random(n) < 0.70, rng.random(n) < 0.15),
        "user_is_minor":        rng.random(n) < 0.02,
        "is_first_transaction": rng.random(n) < 0.10,
        "is_new_card":          np.where(is_fraud, rng.random(n) < 0.45, rng.random(n) < 0.08),
        "is_new_merchant":      np.where(is_fraud, rng.random(n) < 0.50, rng.random(n) < 0.10),
        "ip_geo_mismatch":      np.where(is_fraud, rng.random(n) < 0.65, rng.random(n) < 0.05),
        "is_fraud":             is_fraud,
    })
    return df
 
# ── Définition des features ───────────────────────────────────────────────
NUMERIC_FEATURES = [
    "txn_count_5min", "txn_count_1h", "txn_count_30d",
    "avg_amount_30d", "amount_vs_avg_ratio", "amount_stddev_30d",
    "geo_distance_km",
]
CATEGORICAL_FEATURES = [
    "kyc_status", "age_group", "risk_category", "category_id",
    "payment_method", "billing_country_iso", "shipping_country_iso",
]
BINARY_FEATURES = [
    "is_cross_border", "user_is_minor", "is_first_transaction",
    "is_new_card", "is_new_merchant", "ip_geo_mismatch",
]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES + BINARY_FEATURES

def prepare_features(df: pd.DataFrame):
    """Encode, impute et retourne X, y prêts pour XGBoost."""
    encoders = {}
    for col in CATEGORICAL_FEATURES:
        if col in df.columns:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].fillna("unknown").astype(str))
            encoders[col] = le
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())
    for col in BINARY_FEATURES:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(int)
    feat_cols = [c for c in ALL_FEATURES if c in df.columns]
    return df[feat_cols], df["is_fraud"].astype(int), encoders, feat_cols
 
# ── Entraînement avec tracking MLflow ────────────────────────────────────
def train_and_register():
    df = load_features_from_adls()
    X, y, encoders, feat_cols = prepare_features(df)
 
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
 
    # Calcul de scale_pos_weight pour le déséquilibre des classes
    neg, pos = (y_train == 0).sum(), (y_train == 1).sum()
    ratio = neg / pos
    print(f"scale_pos_weight = {ratio:.1f}  (légitimes: {neg}, fraudes: {pos})")
 
    # ── Hyperparamètres ──────────────────────────────────────────────────
    # Ces valeurs sont le résultat d'une optimisation Optuna préalable.
    # Voir ml/fraud_detection/hyperopt.py pour reproduire la recherche.
    params = {
        "n_estimators":     500,
        "max_depth":        6,
        "learning_rate":    0.05,
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 3,
        "reg_alpha":        0.1,
        "reg_lambda":       1.0,
        "scale_pos_weight": ratio,   # CRUCIAL : correction déséquilibre
        "eval_metric":      "aucpr", # AUC-PR > AUC-ROC sur classes rares
        "random_state":     42,
        "tree_method":      "hist",  # hist = plus rapide sur CPU que exact
        "device":           "cpu",
    }
 
    with mlflow.start_run(run_name="xgboost-fraud-v1") as run:
        print(f"MLflow run_id : {run.info.run_id}")
 
        # Entraînement avec early stopping sur l'ensemble de validation
        model = xgb.XGBClassifier(**params, early_stopping_rounds=20)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=50,   # log toutes les 50 itérations
        )
 
        # ── Évaluation ────────────────────────────────────────────────────
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]
 
        metrics = {
            "f1_score":   f1_score(y_test, y_pred),
            "roc_auc":    roc_auc_score(y_test, y_prob),
            "precision":  precision_score(y_test, y_pred),
            "recall":     recall_score(y_test, y_pred),
            "fraud_rate_test": float(y_test.mean()),
        }
        for k, v in metrics.items():
            print(f"  {k:25s}: {v:.4f}")
 
        # ── SHAP Values — Interprétabilité ────────────────────────────────
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test[:300])
        shap.summary_plot(shap_values, X_test[:300],
                          feature_names=feat_cols, show=False)
        plt.savefig("/tmp/shap_summary.png", dpi=120, bbox_inches="tight")
        plt.close()
 
        # ── Log dans MLflow ────────────────────────────────────────────────
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        mlflow.log_artifact("/tmp/shap_summary.png", "shap")
        mlflow.log_dict({"feature_cols": feat_cols}, "feature_config.json")
        mlflow.log_text(
            classification_report(y_test, y_pred),
            "classification_report.txt"
        )
 
        # ── Enregistrement dans le Model Registry ─────────────────────────
        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )
        print(f"Modèle enregistré : {MODEL_NAME}")
 
        # ── Promotion automatique si F1 >= seuil ──────────────────────────
        F1_THRESHOLD = 0.90
        if metrics["f1_score"] >= F1_THRESHOLD:
            client  = mlflow.tracking.MlflowClient()
            latest  = client.get_latest_versions(MODEL_NAME, stages=["None"])[0]
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=latest.version,
                stage="Production",
                archive_existing_versions=True,   # archive l'ancien Champion
            )
            print(f"✓ Modèle promu en PRODUCTION (version {latest.version})")
        else:
            print(f"⚠ F1 = {metrics['f1_score']:.3f} < {F1_THRESHOLD} → modèle NON promu.")
            print("  Investiguer les features ou ajuster les hyperparamètres.")
 
        return run.info.run_id
 
if __name__ == "__main__":
    run_id = train_and_register()
    print(f"Run terminé : {run_id}")
    print("Ouvrir http://localhost:5000 pour visualiser les résultats.")
