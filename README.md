# LedgerLake

**Financial Data Lakehouse — de l'ingestion temps réel aux données métier gouvernées.**

## 🎯 Le problème

Une institution financière génère chaque jour des millions d'événements — transactions, paiements, virements, opérations de crédit — provenant de systèmes hétérogènes (core banking, gateway de paiement, mobile banking, CRM). Chaque système a son propre format, son propre schéma, sa propre fréquence. Sans plateforme unifiée, les équipes Risk, BI et Data Science travaillent chacune sur des données incohérentes, non gouvernées et difficiles à tracer.

## 💡 La solution

LedgerLake est un data lakehouse financier qui centralise, nettoie et structure ces données en suivant l'architecture **Medallion** (Bronze/Silver/Gold), aujourd'hui le standard des plateformes de données modernes :

- Les événements financiers sont ingérés en temps réel via **Kafka**, multi-topics (transactions, clients, crédits, fraude)
- Ils sont stockés bruts et immuables en couche **Bronze** (data lake S3-compatible)
- **Spark** les nettoie, déduplique, type et standardise en couche **Silver**
- **dbt** transforme ces données propres en tables métier prêtes à l'emploi en couche **Gold** (`customer_360`, `loan_portfolio_analysis`, `fraud_detection_metrics`)
- Chaque étape est validée par **Great Expectations**, orchestrée par **Airflow**, et cataloguée par **OpenMetadata** pour une traçabilité complète (data lineage)

## 🏗️ Ce que le projet démontre

| Domaine | Compétences illustrées |
|---|---|
| **Architecture de données** | Medallion Architecture (Bronze/Silver/Gold), conception de lakehouse |
| **Streaming & Ingestion** | Architecture event-driven multi-topics (Kafka), découplage des systèmes sources |
| **Traitement distribué** | Transformation à l'échelle avec Apache Spark |
| **Analytics Engineering** | Modélisation SQL versionnée et testée avec dbt |
| **Data Quality** | Validation automatisée à chaque frontière de couche (Great Expectations) |
| **Orchestration** | Pipelines complexes multi-étapes avec dépendances (Airflow) |
| **Data Governance** | Catalogue de données, lignée, propriété des tables (OpenMetadata) |
| **Finance** | Modélisation de cas d'usage réels : risque crédit, détection de fraude, analytics client |

## 🧱 Stack technique

`Apache Kafka` · `Apache Spark` · `MinIO (S3)` · `dbt` · `Apache Airflow` · `Great Expectations` · `OpenMetadata` · `Prometheus` · `Grafana` · `Docker`

## 📐 Architecture

Voir `docs/GUIDE_COMPLET.md` pour l'architecture détaillée et la méthodologie de construction en paliers.

## 🔗 Projet complémentaire

LedgerLake alimente en données propres et gouvernées le moteur de scoring crédit **CreditPulse**, formant ensemble une architecture complète de plateforme financière : *données → décision*.
