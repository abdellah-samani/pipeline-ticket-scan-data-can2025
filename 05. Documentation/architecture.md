## **architecture.md** (Détails techniques)


# 🏗️ Architecture du projet CAN 2025

## 📊 Architecture globale
```markdown

┌─────────────────────────────────────────────────────────────────────┐
│                        COUCHE CONSOMMATION                          │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │                    Power BI Dashboard                      │     │
│  │  • Connexion via Unity Catalog                             │     │
│  │                                                            │     │
│  │                                                            │     │
│  └────────────────────────────────────────────────────────────┘     │
│                              │                                      │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
┌──────────────────────────────┼──────────────────────────────────────┐
│                        COUCHE GOLD (Modélisée)                      │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │                    Schéma en étoile                        │     │
│  │  • fact_ticket_scan (partitionnée par mois)                │     │
│  │  • dim_stadium, dim_team, dim_match, dim_time, dim_fan     │     │
│  │  • Format Delta                                            │     │
│  │  • Clés entières pour performance                          │     │
│  └────────────────────────────────────────────────────────────┘     │
│                              │                                      │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
┌──────────────────────────────┼──────────────────────────────────────┐
│                        COUCHE SILVER (Nettoyée)                     │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │                    Databricks Notebooks                    │     │
│  │  • 01 - Nettoyage dimensions                               │     │
│  │  • 02 - Nettoyage faits (1.46M lignes)                     │     │
│  │  • 03 - Enrichissement                                     │     │
│  │  • Format Delta                                            │     │
│  │  • Audit des données rejetées                              │     │
│  └────────────────────────────────────────────────────────────┘     │
│                              │                                      │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
┌──────────────────────────────┼──────────────────────────────────────┐
│                        COUCHE BRONZE (Brute)                        │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │                    Azure Data Factory                      |     │
│  │  • Pipeline paramétré                                      │     │
│  │  • Lookup dynamique                                        │     │
│  │  • ForEach parallèle                                       │     │
│  │  • Format Parquet                                          │     │
│  └────────────────────────────────────────────────────────────┘     │
│                              │                                      │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                     SOURCES DE DONNÉES                              │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │  Azure SQL Databases:                                      │     │
│  │  • db_caf_competition (équipes, matchs)                    │     │
│  │  • db_gov_infrastructure (stades, villes)                  │     │
│  └────────────────────────────────────────────────────────────┘     │
│  ┌────────────────────────────────────────────────────────────┐     │ 
│  │  36 fichiers CSV:                                          │     │
│  │  • 1 fichier par match                                     │     │
│  │  • Scans de billets simulés (données générées)             │     │
│  └────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
```

## 🔄 Flux de données

### 1. Ingestion (ADF)
```
Source → Lookup Config → ForEach → Copy → Bronze (Parquet)
      
```

**Pipeline ADF** : `pl_to_bronze_parquet.json`
- **Lookup** : Lecture des fichiers de configuration
- **ForEach SQL** : Ingestion des tables SQL en parallèle
- **ForEach CSV** : Ingestion des fichiers CSV en parallèle


### 2. Transformation Silver (Databricks)
```
Bronze (Parquet) → Notebooks → Silver (Delta) → Audit (Delta)

Les notebooks Databricks sont exécutés de manière séquentielle (Silver → Gold).
L’orchestration complète via ADF est identifiée comme une amélioration future.
```

**Notebook 1** : `01_silver_dimensions_processing.py`
- Nettoyage des tables de dimensions
- Normalisation des formats
- Correction des heures de match

**Notebook 2** : `02_silver_fact_tickets_cleaning.py`
- Nettoyage de 1.46M scans de billets
- Validation des formats (ticket_id, fan_id)
- Score de qualité : 99.88% valides

**Notebook 3** : `03_silver_fact_tickets_enriching.py`
- Enrichissement des match_id manquants
- Correction des stadium_id NULL
- Préparation pour la modélisation

### 3. Modélisation Gold (Databricks)
```
Silver (Delta) → Star Schema → Gold (Delta) → Power BI
```

**Notebook 4** : `04_gold_star_schema_creation.py`
- Création de 5 dimensions dénormalisées
- Création de la table de faits partitionnée
- Clés entières pour optimisation

## 🗄️ Structure des données

### Bronze Layer (Parquet)
```
/bronze/
├── competition_data/
│   ├── teams_raw.parquet
│   └── matches_raw.parquet
│    
├── infrastructure_data/
│   ├── stadiums_raw.parquet
│   └── cities_raw.parquet
│    
└── group_stage_tickets_data/
    ├── [Stadium_name].parquet
    ├── [Stadium_name].parquet
    └── ...
```

### Silver Layer (Delta)
```
/silver/
|
├── stadiums_clean.delta
├── teams_clean.delta
├── cities_clean.delta
├── matches_clean.delta
└── ticket_scans_clean.delta


/audit/
    ├── ticket_id_invalide.delta
    ├── fan_id_invalide.delta
    └── ...
```

### Gold Layer (Delta - Star Schema)
```
/gold/
├── dim_stadium/
│   ├── _delta_log/
│   └── part-*.parquet
├── dim_team/
├── dim_match/
├── dim_time/
├── dim_fan/
└── fact_ticket_scan/
    ├── scan_day=2025_12_21/
    ├── scan_day=2025_12_22/
    └── ...
```

## ⚙️ Configuration technique

### Clusters Databricks
- **Type** : Standard_DS4ds_v4 (16GB Memory, 4 Cores)
- **Workers** : Single Node
- **Runtime** : 17.3 LTS (includes Apache Spark 4.0.0, Scala 2.13)
- **Libraries** : Delta Lake, PySpark

### Pipeline ADF
- **Timeout** : 12 heures maximum
- **Retry** : 0 (reprise gérée au niveau métier)
- **Parallel copies** : 10 maximum
- **Integration Runtime** : Auto-resolve

### Stockage ADLS
- **Redondance** : LRS (Locally Redundant Storage)
- **Niveau d'accès** : Hot
- **Chiffrement** : Microsoft Managed Keys
- **Networking** : Private Endpoints recommandés

## 🔐 Sécurité et gouvernance

### Azure Key Vault
```
Secrets stockés :
├── key-adls-can2025
└── key-sql-can2025
```


## Améliorations futures
- CI/CD avec Azure DevOps
- Monitoring avec Azure Monitor
- Data Catalog avec Purview

---
