# 🏆 CAN 2025 – Pipeline pour l’ingestion, la transformation et l’analyse des données des scans de billets – CAN 2025

## 📋 Aperçu du projet
Pipeline complet d'ingestion, transformation et analyse des données **simulées** de scans de billets pour la Coupe d'Afrique des Nations 2025.

**Auteur** : Abdellah Samani  
**Email** : abdellah.samani.data@gmail.com  
**Date** : Janvier 2026

---

## 🎯 Objectifs
- Centraliser les sources hétérogènes (bases SQL + fichiers CSV)
- Construire un pipeline fiable, scalable et automatisé
- Produire des données prêtes pour l'analyse (gold layer)
- Assurer audit et traçabilité des données
- Démontrer l'utilisation des services Azure pour un projet complet de data engineering

---

## 📊 Résultats
| Métrique | Valeur |
|----------|--------|
| Données traitées | 1.46 million de scans |
| Qualité données | 99.88% valides |

---

##  Architecture

**Architecture Médaillon :**
Bronze (Parquet) → Silver (Delta) → Gold (Star Schema Delta)


### Stack technique :
- **Ingestion** : Azure Data Factory
- **Transformation** : Azure Databricks (PySpark)
- **Stockage** : Azure Data Lake Gen2
- **Visualisation** : Power BI
- **Sécurité** : Azure Key Vault
- **Format** : Parquet → Delta → Delta



## 📁 Structure du projet

CAN 2025 - Pipeline Data Engineering End-to-End/
├── 01. Source de Données (SQL et CSV)/                # Données utilisés dans le projet
├── 02. Ingestion (Azure Data Factory Pipeline)/       # Pipelines ADF et configurations
├── 03. Transformation (Azure Databricks Notebooks)/   # Notebooks Databricks
├── 04. Consommation (Power BI Dashboard)              # Fichiers Power BI (capture d'écran + fichier pbix)
├── 05. Documentation/                                 # Documentation et diagrammes
├── 06. Screenshots/                                   # Captures d'écran du portail Azure
└── README.md


## 🔧 Composants principaux

### Ingestion (ADF)
- `pl_to_bronze_parquet` : Pipeline principal d'ingestion
- Configuration dynamique via fichiers CSV

### Transformation (Databricks)
- `01_silver_dimensions_processing` : Nettoyage des dimensions
- `02_silver_fact_tickets_cleaning` : Nettoyage des faits (1.46M lignes)
- `03_silver_fact_tickets_enriching` : Enrichissement des données
- `04_gold_star_schema_creation` : Création du schéma en étoile

### Consommation (Power BI)
- Dashboard analytique basé sur données batch
- Connexion via Unity Catalog
- KPI : billets scannés, participation par stade, répartition par canal

---

## 📈 Métriques de qualité
- **Complétude** : 100% des champs obligatoires
- **Validité** : Formats validés (ticket_id, fan_id, dates)
- **Cohérence** : Relations référentielles vérifiées
- **Unicité** : Pas de doublons dans les clés

---

## 🛡️ Sécurité
- Tous les secrets stockés dans **Azure Key Vault**
- Accès aux données contrôlé et audité

---


---

## 📄 Licence
Ce projet est sous licence **MIT**. Voir le fichier [LICENSE](LICENSE) pour plus de détails.
