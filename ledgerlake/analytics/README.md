# analytics/ — Connexion BI / ML (Palier H, bonus)

À IMPLÉMENTER :
1. Connexion Power BI (ou Metabase pour une démo 100% locale et gratuite)
   sur les tables Gold (customer_360, loan_portfolio_analysis,
   fraud_detection_metrics)
2. Dashboard de démonstration : transactions du jour, volume total,
   taux de fraude, top clients par risque

## Connexion à CreditPulse

Pour fermer la boucle "plateforme de données -> moteur de décision" :
1. Le backend FastAPI de CreditPulse lit les features directement depuis
   la table Gold `loan_portfolio_analysis` / `customer_360` au lieu de
   données simulées
2. Le modèle XGBoost de CreditPulse peut être ré-entraîné périodiquement
   sur les données fraîches de LedgerLake (via une tâche Airflow commune)

C'est cette connexion qui transforme deux projets séparés en une véritable
architecture de plateforme financière cohérente pour ton portfolio.
