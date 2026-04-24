# Makefile
# Makefile (racine du projet)
.PHONY: help kafka-start kafka-stop airflow-start airflow-stop status clean
 
help:  ## Afficher l'aide
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | sort
 
kafka-start:  ## Démarrer Kafka + Kafka UI
	cd docker && docker compose -f docker-compose.kafka.yml up -d
	@echo 'Kafka UI : http://localhost:8090'
 
kafka-stop:  ## Arrêter Kafka
	cd docker && docker compose -f docker-compose.kafka.yml down
 
airflow-start:  ## Démarrer Airflow
	cd docker && docker compose -f docker-compose.airflow.yml up -d
	@echo 'Airflow UI : http://localhost:8080 (admin/admin)'
 
airflow-stop:  ## Arrêter Airflow
	cd docker && docker compose -f docker-compose.airflow.yml down
 
start-all:  ## Démarrer tout l'environnement local
	$(MAKE) kafka-start
	@sleep 20
	$(MAKE) airflow-start
	@echo 'Environnement complet démarré'
 
stop-all:  ## Arrêter tout
	$(MAKE) airflow-stop
	$(MAKE) kafka-stop
 
status:  ## Vérifier l'état des services
	@echo '=== KAFKA ==='
	@docker ps --filter name=kivendtout-kafka --format 'table {{.Names}}\t{{.Status}}'
	@echo '=== AIRFLOW ==='
	@docker ps --filter name=kivendtout-airflow --format 'table {{.Names}}\t{{.Status}}'
 
dbt-test:  ## Lancer les tests dbt
	cd dbt && dbt test
 
dbt-docs:  ## Générer et servir la doc dbt
	cd dbt && dbt docs generate && dbt docs serve
 
