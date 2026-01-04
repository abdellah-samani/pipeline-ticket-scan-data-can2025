#  Catalogue de Données - Projet CAN 2025

## 1. Tables Principales

### 1.1 dim_fan
| Colonne             | Type       | Description                              |
|---------------------|------------|------------------------------------------|
| fan_key             | LONG       | Identifiant unique du fan                |
| fan_id              | STRING     | Identifiant original du fan (source)     |
| fan_category        | STRING     | Catégorie du fan (Identified or not)     |
| fan_type            | STRING     | Type du fan                              |
| load_ts             | TIMESTAMP  | Timestamp d'ingestion                    |

### 1.2 dim_team
| Colonne             | Type       | Description                              |
|---------------------|------------|------------------------------------------|
| team_key            | STRING     | Code unique de l'équipe                  |
| team_name           | STRING     | Nom de l'équipe                          |
| team_group          | STRING     | Groupe de l'équipe (A, B, ...)           |
| federation          | STRING     | Fédération nationale                     |
| coach_name          | STRING     | Nom de l'entraîneur                      |
| load_ts             | TIMESTAMP  | Timestamp d'ingestion                    |

### 1.3 dim_match
| Colonne             | Type       | Description                              |
|---------------------|------------|------------------------------------------|
| match_key           | STRING     | Identifiant unique du match              |
| match_date          | DATE       | Date du match                            |
| match_time          | STRING     | Heure du match (HH:MM)                   |
| match_datetime      | TIMESTAMP  | Date et heure combinées                  |
| stage               | STRING     | Phase du tournoi                         |
| match_group         | STRING     | Groupe du match                          |
| stadium_id          | STRING     | Stade où se déroule le match             |
| stadium_name        | STRING     | Nom du stade                             |
| home_team_name      | STRING     | Équipe à domicile                        |
| away_team_name      | STRING     | Équipe visiteuse                         |
| weather_notes       | STRING     | Notes météo                              |
| load_ts             | TIMESTAMP  | Timestamp d'ingestion                    |

### 1.4 dim_stadium
| Colonne             | Type       | Description                              |
|---------------------|------------|------------------------------------------|
| stadium_key         | STRING     | Identifiant unique du stade              |
| stadium_name        | STRING     | Nom du stade                             |
| city_code           | STRING     | Code de la ville                         |
| city_name           | STRING     | Nom de la ville                          |
| region              | STRING     | Région                                   |
| full_address        | STRING     | Adresse complète                         |
| seating_capacity    | INT        | Capacité assise                          |
| pitch_type          | STRING     | Type de terrain (gazon, synthétique)     |
| load_ts             | TIMESTAMP  | Timestamp d'ingestion                    |

### 1.5 dim_date
| Colonne             | Type       | Description                              |
|---------------------|------------|------------------------------------------|
| date_key            | INT        | Identifiant unique de la date            |
| event_date          | DATE       | Date réelle de l’événement               |
| year                | INT        | Année                                    |
| month               | INT        | Mois (numérique)                         |
| month_name          | STRING     | Nom du mois                              |
| day                 | INT        | Jour du mois                             |
| day_type            | STRING     | Type de jour (weekday, weekend)          |
| load_ts             | TIMESTAMP  | Timestamp d'ingestion                    |

### 1.6 fact_ticket_scan
| Colonne             | Type       | Description                              |
|---------------------|------------|------------------------------------------|
| ticket_scan_key     | LONG       | Identifiant unique du scan               |
| match_key           | STRING     | Référence vers dim_match                 |
| stadium_key         | STRING     | Référence vers dim_stadium               |
| fan_key             | LONG       | Référence vers le fan                    |
| date_key            | INT        | Référence vers dim_time                  |
| home_team_key       | STRING     | Référence vers dim_team                  |
| away_team_key       | STRING     | Référence vers dim_team                  |
| ticket_id           | STRING     | Identifiant du ticket                    |
| ticket_category     | STRING     | Catégorie du ticket                      |
| ticket_price_mad    | INT        | Prix en MAD                              |
| purchase_channel    | STRING     | Canal d'achat (online, box office)       |
| scan_status         | STRING     | Statut du scan (validé, refusé)          |
| scan_timestamp      | TIMESTAMP  | Timestamp du scan                        |
| entry_gate          | STRING     | Porte d’entrée                           |
| scan_day            | STRING     | Date du scan                             |
| load_ts             | TIMESTAMP  | Timestamp d'ingestion                    |


---

## 2. Relations

- **fact_ticket_scan → dim_fan** : `fan_key`  
- **fact_ticket_scan  → dim_match** : `match_key`  
- **fact_ticket_scan → dim_team** : `home_team_key`, `away_team_key`  
- **fact_ticket_scan → dim_stadium** : `stadium_key`  
- **fact_ticket_scan → dim_date** : `date_key`  


---

## 3. Type d'architecture
- **Modèle en étoile (Star Schema)** avec la table fact `fact_ticket_scan` et les dimensions `dim_fan`, `dim_match`, `dim_team`, `dim_stadium`, `dim_date`.
- **Medallion Architecture** appliquée :
  - Bronze : données brutes
  - Silver : données nettoyées et harmonisées
  - Gold : Modélisation schéma en étoile
