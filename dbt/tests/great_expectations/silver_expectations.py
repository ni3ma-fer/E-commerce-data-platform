# tests/great_expectations/silver_expectations.py
from datetime import datetime, timedelta

# Calcul dynamique des dates pour Great Expectations
today_str = datetime.today().strftime('%Y-%m-%d')
yesterday_str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

# ── Expectations Silver Paiements ──────────────────────────────
silver_payments_expectations = [
    # Clés : obligatoires et uniques
    {'type': 'expect_column_values_to_not_be_null',  'kwargs': {'column': 'transaction_id'}},
    {'type': 'expect_column_values_to_be_unique',    'kwargs': {'column': 'transaction_id'}},
    {'type': 'expect_column_values_to_not_be_null',  'kwargs': {'column': 'user_id'}},

    # Montant : strictement positif, pas de valeur extrême sans alerte
    {'type': 'expect_column_values_to_be_between',
     'kwargs': {'column': 'amount_eur', 'min_value': 0.01, 'max_value': 99999}},

    # Devise : uniquement les devises supportées (CORRECTION : ajout de l'AUD)
    {'type': 'expect_column_values_to_be_in_set',
     'kwargs': {'column': 'currency_code', 'value_set': ['EUR', 'USD', 'GBP', 'CHF', 'JPY', 'CAD', 'AUD']}},

    # Pays : format ISO 2 lettres
    {'type': 'expect_column_value_lengths_to_equal',
     'kwargs': {'column': 'billing_country_iso', 'value': 2}},

    # RGPD : vérifier que les emails ne sont PLUS en clair (colonne supprimée)
    {'type': 'expect_column_to_not_exist',
     'kwargs': {'column': 'email'}},

    # RGPD : vérifier que les prénoms ne sont PLUS en clair
    {'type': 'expect_column_to_not_exist',
     'kwargs': {'column': 'first_name'}},

    # RGPD : vérifier que la pseudonymisation Presidio a bien été appliquée
    {'type': 'expect_column_to_exist',
     'kwargs': {'column': 'pii_anonymized_at'}},

    # Timestamps valides (pas de dates dans le futur) - CORRECTION DYNAMIQUE
    {'type': 'expect_column_values_to_be_between',
     'kwargs': {'column': 'event_timestamp',
                'min_value': '2020-01-01', 'max_value': today_str, 'parse_strings_as_datetimes': True}},

    # Volumétrie : Silver ne peut pas être vide (signe d'un problème pipeline)
    {'type': 'expect_table_row_count_to_be_between',
     'kwargs': {'min_value': 1, 'max_value': 10000000}},

    # Fraîcheur des données : pas de Silver plus vieux que 26h - CORRECTION DYNAMIQUE
    {'type': 'expect_column_max_to_be_between',
     'kwargs': {'column': 'silver_loaded_at',
                'min_value': yesterday_str, 'max_value': today_str, 'parse_strings_as_datetimes': True}},
]