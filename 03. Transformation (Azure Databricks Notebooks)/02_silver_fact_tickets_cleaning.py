# Databricks notebook source
# MAGIC %md
# MAGIC ##  Notebook : 02_silver_tickets_cleaning
# MAGIC   - **Objectif** : Nettoyer le dataset des billets pour Silver Layer  
# MAGIC   - **Transformations principales** :
# MAGIC     1. Nettoyage et déduplication des données
# MAGIC     2. Normalisation des `ticket_id` et `fan_id`
# MAGIC     3. Validations du catégories, prix, canaux, dates
# MAGIC   - **Lecture directe depuis ADLS sans mounts**
# MAGIC   ________________________________________________________________________________

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports des librairies
# MAGIC ___________________________

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration ADLS
# MAGIC _________________________

# COMMAND ----------

# Définir les informations d'accès ADLS
storage_account_name = "adlscan2025"
container_name_bronze = "bronze"
container_name_silver = "silver"
container_name_audit = "audit"
secret_scope = "secret-scope-can2025"
secret_name = "key-adls-can2025"

# Chemins ABFSS
bronze_path = f"abfss://{container_name_bronze}@{storage_account_name}.dfs.core.windows.net/group_stage_tickets_data"
silver_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/ticket_scans_clean.delta"
audit_base_path = f"abfss://{container_name_audit}@{storage_account_name}.dfs.core.windows.net/tickets_scan/dropped"

# Configurer les credentials pour accéder à ADLS
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=secret_scope, key=secret_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chargement des données Bronze
# MAGIC __________________________________

# COMMAND ----------

# Lire le dataset Bronze
tickets_df = spark.read.format("parquet").load(bronze_path)
print(f" Nombre de lignes chargées depuis Bronze: {tickets_df.count():,}")

# Afficher un aperçu initial
print("\n Aperçu des données initiales :")
tickets_df.limit(5).display()


# COMMAND ----------

# MAGIC %md 
# MAGIC ##  Phase 1 : Nettoyage des données
# MAGIC ________________________________________________________________

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1 Supprimer les doublons complets
# MAGIC ___________________________________________________________________________

# COMMAND ----------

# Identifier les doublons complets
duplicates_df = tickets_df.groupBy(tickets_df.columns).count().filter("count > 1")

if duplicates_df.count() > 0:
    duplicates_df.withColumn("audit_reason", F.lit("ligne double")).withColumn(
        "audit_date", F.current_timestamp()
    ).write.mode("overwrite").option("mergeSchema", "true").format("delta").save(
        f"{audit_base_path}/complets_doublons"
    )
    print(f"{duplicates_df.count()} lignes en double sauvegardées dans audit")


# COMMAND ----------

# Supprimer doublons
tickets_df_clean = tickets_df.dropDuplicates()
print(f"Doublons complets supprimés")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2 Supprimer les valeurs nulles dans ticket_id
# MAGIC ___________________________________________________________________________

# COMMAND ----------

null_ticket_df = tickets_df_clean.filter(F.col("ticket_id").isNull())
if null_ticket_df.count() > 0:
    null_ticket_df.withColumn("audit_reason", F.lit("ticket_id nulle")).withColumn(
        "audit_date", F.current_timestamp()
    ).write.mode("overwrite").format("delta").save(f"{audit_base_path}/ticket_id_null")
    print(
        f"{null_ticket_df.count()} lignes avec ticket_id null sauvegardées dans audit"
    )

tickets_df_clean = tickets_df_clean.filter(F.col("ticket_id").isNotNull())
print(f"Tickets avec ticket_id NULL supprimés")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.3 Supprimer les doublons par ticket_id (conserver le premier scanné)
# MAGIC _____________________________________________________________________________

# COMMAND ----------

window_spec = Window.partitionBy("ticket_id").orderBy("scan_timestamp")
tickets_with_rownum = tickets_df_clean.withColumn("row_num", F.row_number().over(window_spec))

# Sauvegarder les tickets_id dupliqués
duplicates_ticketid_df = tickets_with_rownum.filter(F.col("row_num") > 1)
if duplicates_ticketid_df.count() > 0:
    duplicates_ticketid_df.withColumn("audit_reason", F.lit("ticket_id dupliqué")).withColumn(
        "audit_date", F.current_timestamp()
    ).write.mode("overwrite").format("delta").save(f"{audit_base_path}/ticket_id_doublons")
    print(f" {duplicates_ticketid_df.count()} lignes en double par ticket_id sauvegardées dans audit")

tickets_df_clean = tickets_with_rownum.filter(F.col("row_num") == 1).drop("row_num")
print(f"Doublons par ticket_id supprimés")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Phase 2 : Normalisation des ticket_id
# MAGIC ________________________________________________________________

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1 Normalisation de ticket_id
# MAGIC ______________________________________________________

# COMMAND ----------

tickets_norm = (
    tickets_df_clean
    # suppression espaces
    .withColumn("ticket_id", F.trim(F.col("ticket_id")))
    # mise en majuscule
    .withColumn("ticket_id", F.upper(F.col("ticket_id")))
)

print("Normalisation de base des ticket_id terminée")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2 Correction automatique des formats ticket_id
# MAGIC   
# MAGIC   **Cas corrigés :**
# MAGIC - `ticket_xxxxx` → `TICKET_XXXXX`
# MAGIC - `TK_12345678` → `TICKET_12345678`
# MAGIC - `TICKET12345` → `TICKET_12345`
# MAGIC ________________________________________________________________

# COMMAND ----------

tickets_fixed = tickets_norm.withColumn(
    "ticket_id_fixed",
    F.when(F.col("ticket_id").rlike("^TICKET_[A-Z0-9]+$"), F.col("ticket_id"))
     .when(F.col("ticket_id").rlike("^TICKET[A-Z0-9]+$"),
           F.regexp_replace(F.col("ticket_id"), "^TICKET", "TICKET_"))
     .when(F.col("ticket_id").rlike("^TK_[A-Z0-9]+$"),
           F.regexp_replace(F.col("ticket_id"), "^TK_", "TICKET_"))
     .otherwise(None)
)

print("Correction automatique des formats ticket_id terminée")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3 Validation des ticket_id
# MAGIC ________________________________________________

# COMMAND ----------

tickets_validated = tickets_fixed.withColumn(
    "ticket_id_valide",
    F.col("ticket_id_fixed").rlike("^TICKET_[A-Z0-9]{3,12}$")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.4 Séparation des tickets valides et rejetés
# MAGIC ________________________________________________
# MAGIC

# COMMAND ----------

tickets_rejetes = tickets_validated.filter(~F.col("ticket_id_valide"))
tickets_valides = tickets_validated.filter(F.col("ticket_id_valide"))

print(f"Tickets valides : {tickets_valides.count():,}")
print(f"Tickets rejetés : {tickets_rejetes.count():,}")

# Sauvegarder les tickets rejetés
if tickets_rejetes.count() > 0:
    tickets_rejetes.withColumn("audit_reason", F.lit("ticket_id invalide")).withColumn(
        "audit_date", F.current_timestamp()
    ).write.mode("append").format("delta").save(f"{audit_base_path}/ticket_id_invalide")
    print("Tickets rejetés sauvegardés dans audit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3 : Nettoyage fan_id (Silver)
# MAGIC ________________________________________________________________

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1 Normalisation de base
# MAGIC _________________________________

# COMMAND ----------

tickets_fan_norm = tickets_valides.withColumn(
    "fan_id",
    F.when(F.col("fan_id").isNull(), F.lit("N/A"))
     .otherwise(F.upper(F.trim(F.col("fan_id"))))
)

print("Normalisation de base des fan_id terminée")

# COMMAND ----------

tickets_fan_fixed = tickets_fan_norm.withColumn(
    "fan_id_fixed",
    F.when(F.col("fan_id") == "N/A", F.lit("N/A"))

     # FAN_XXXXXXXX (déjà valide)
     .when(F.col("fan_id").rlike("^FAN_[A-Z0-9]{3,12}$"), F.col("fan_id"))

     # FANXXXXXXXX → FAN_XXXXXXXX
     .when(F.col("fan_id").rlike("^FAN[A-Z0-9]{3,12}$"),
           F.regexp_replace(F.col("fan_id"), "^FAN", "FAN_"))

     # fanxxxxxxxx → FAN_XXXXXXXX
     .when(F.col("fan_id").rlike("^FAN[A-Z0-9]{3,12}$"),
           F.regexp_replace(F.col("fan_id"), "^FAN", "FAN_"))

     # fan_XXXXXXXX → FAN_XXXXXXXX
     .when(F.col("fan_id").rlike("^FAN_[A-Z0-9]{3,12}$"),
           F.col("fan_id"))

     .otherwise(None)
)

print("Correction des formats fan_id terminée")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.3 Validation des fan_id
# MAGIC ________________________________________________

# COMMAND ----------

tickets_fan_validated = tickets_fan_fixed.withColumn(
    "fan_id_valide",
    (F.col("fan_id_fixed") == "N/A") |
    (F.col("fan_id_fixed").rlike("^FAN_[A-Z0-9]{3,12}$"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4 Séparation des fan_id valides et rejetés
# MAGIC ________________________________________________

# COMMAND ----------

fans_rejetes = tickets_fan_validated.filter(~F.col("fan_id_valide"))
fans_valides = tickets_fan_validated.filter(F.col("fan_id_valide"))

print(f"fan_id valides : {fans_valides.count():,}")
print(f"fan_id rejetés : {fans_rejetes.count():,}")

# Sauvegarder les fans rejetés
if fans_rejetes.count() > 0:
    fans_rejetes.withColumn("audit_reason", F.lit("fan_id invalide")).withColumn(
        "audit_date", F.current_timestamp()
    ).write.mode("append").format("delta").save(f"{audit_base_path}/fan_id_invalide")
    print("Fans rejetés sauvegardés dans audit")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5 Déduplication fan_id par match
# MAGIC ________________________________________________

# COMMAND ----------

fans_identifies = fans_valides.filter(F.col("fan_id") != "N/A")
fans_anonymes = fans_valides.filter(F.col("fan_id") == "N/A")

print(f"Fans identifiés : {fans_identifies.count():,}")
print(f"Fans anonymes : {fans_anonymes.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.6 Détection des doublons fan_id + match_id
# MAGIC ________________________________________________

# COMMAND ----------

window_fan_match = Window.partitionBy("match_id", "fan_id") \
                          .orderBy("scan_timestamp")

fans_with_rownum = fans_identifies.withColumn(
    "row_num",
    F.row_number().over(window_fan_match)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.7 Séparer valides / doublons frauduleux
# MAGIC ________________________________________________

# COMMAND ----------

fans_valides_unique = fans_with_rownum.filter(F.col("row_num") == 1).drop("row_num")
fans_doublons = fans_with_rownum.filter(F.col("row_num") > 1).drop("row_num")

print(f"Entrées valides (fan_id unique par match) : {fans_valides_unique.count():,}")
print(f"Entrées rejetées (fan_id dupliqué dans le même match) : {fans_doublons.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.8 Sauvegarde des doublons en AUDIT
# MAGIC ________________________________________________

# COMMAND ----------

if fans_doublons.count() > 0:
    (
        fans_doublons
        .withColumn("audit_reason", F.lit("fan_id dupliqué dans le même match"))
        .withColumn("audit_date", F.current_timestamp())
        .write
        .mode("append")
        .format("delta")
        .save(f"{audit_base_path}/fan_id_doublons")
    )
    print("Doublons fan_id sauvegardés dans audit")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.9 Reconstruction du dataset final
# MAGIC ________________________________________________

# COMMAND ----------

tickets_clean_final = fans_valides_unique.unionByName(fans_anonymes)
print(f"Dataset final reconstitué : {tickets_clean_final.count():,} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4 : Nettoyage des données métier
# MAGIC ________________________________________________________________

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Normalisation des catégories de billets
# MAGIC   
# MAGIC   **Valeurs acceptées** : `CATEGORY_1`, `CATEGORY_2`, `CATEGORY_3`
# MAGIC   
# MAGIC   **Règles de normalisation** :
# MAGIC   - Mise en majuscules
# MAGIC   - Suppression des espaces
# MAGIC   - Correction des formats courants
# MAGIC    ________________________________________________

# COMMAND ----------

# Normalisation des catégories
tickets_with_categories = tickets_clean_final.withColumn(
    "ticket_category_clean",
    F.when(F.col("ticket_category").isNull(), F.lit("UNKNOWN"))
     .otherwise(
         F.upper(F.trim(F.col("ticket_category")))
     )
)

# COMMAND ----------

# Correction des formats courants
tickets_with_categories = tickets_with_categories.withColumn(
    "ticket_category_fixed",
    F.when(F.col("ticket_category_clean").isin(["CATEGORY_1", "CATEGORY_2", "CATEGORY_3"]), 
           F.col("ticket_category_clean"))
     .when(F.col("ticket_category_clean").rlike("^CATEGORY[ _]*1$"), F.lit("CATEGORY_1"))
     .when(F.col("ticket_category_clean").rlike("^CATEGORY[ _]*2$"), F.lit("CATEGORY_2"))
     .when(F.col("ticket_category_clean").rlike("^CATEGORY[ _]*3$"), F.lit("CATEGORY_3"))
     .when(F.col("ticket_category_clean").rlike("^CAT[ _]*1$"), F.lit("CATEGORY_1"))
     .when(F.col("ticket_category_clean").rlike("^CAT[ _]*2$"), F.lit("CATEGORY_2"))
     .when(F.col("ticket_category_clean").rlike("^CAT[ _]*3$"), F.lit("CATEGORY_3"))
     .when(F.col("ticket_category_clean") == "1", F.lit("CATEGORY_1"))
     .when(F.col("ticket_category_clean") == "2", F.lit("CATEGORY_2"))
     .when(F.col("ticket_category_clean") == "3", F.lit("CATEGORY_3"))
     .otherwise(F.lit("INVALID"))
)

# COMMAND ----------

# Validation
tickets_with_categories = tickets_with_categories.withColumn(
    "ticket_category_valide",
    F.col("ticket_category_fixed").isin(["CATEGORY_1", "CATEGORY_2", "CATEGORY_3"])
)

print("Résultats de la normalisation des catégories :")
tickets_with_categories.groupBy("ticket_category_fixed", "ticket_category_valide").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# Identifier les catégories invalides
invalid_categories = tickets_with_categories.filter(~F.col("ticket_category_valide"))
if invalid_categories.count() > 0:
    print(f"Catégories invalides détectées : {invalid_categories.count():,}")
    invalid_categories.select("ticket_id", "ticket_category", "ticket_category_fixed").limit(10).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Validation des prix des billets
# MAGIC   
# MAGIC   **Règles** :
# MAGIC   - Prix ne peut pas être négatif
# MAGIC   - Prix doit être cohérent avec la catégorie
# MAGIC   ________________________________________________

# COMMAND ----------

print("Analyse des prix des billets :")

# Statistiques des prix
tickets_with_categories.select(
    F.min("ticket_price_mad").alias("prix_min"),
    F.max("ticket_price_mad").alias("prix_max"),
    F.mean("ticket_price_mad").alias("prix_moyen"),
    F.count(F.when(F.col("ticket_price_mad") < 0, 1)).alias("prix_negatifs"),
    F.count(F.when(F.col("ticket_price_mad").isNull(), 1)).alias("prix_nulls")
).show()

# COMMAND ----------

# Validation des prix
tickets_with_prices = tickets_with_categories.withColumn(
    "ticket_price_valide",
    (F.col("ticket_price_mad").isNotNull()) &
    (F.col("ticket_price_mad") >= 0)
)

# COMMAND ----------

# Vérification de la cohérence prix/catégorie
tickets_with_prices = tickets_with_prices.withColumn(
    "prix_categorie_coherent",
    F.when(
        (F.col("ticket_category_fixed") == "CATEGORY_1") & (F.col("ticket_price_mad").between(250, 350)), True
    ).when(
        (F.col("ticket_category_fixed") == "CATEGORY_2") & (F.col("ticket_price_mad").between(150, 250)), True
    ).when(
        (F.col("ticket_category_fixed") == "CATEGORY_3") & (F.col("ticket_price_mad").between(50, 150)), True
    ).otherwise(False)
)

# COMMAND ----------

print("Validation des prix :")
validation_stats = tickets_with_prices.agg(
    F.count(F.when(F.col("ticket_price_valide"), 1)).alias("prix_valides"),
    F.count(F.when(~F.col("ticket_price_valide"), 1)).alias("prix_invalides"),
    F.count(F.when(F.col("prix_categorie_coherent"), 1)).alias("prix_categorie_coherents"),
    F.count(F.when(~F.col("prix_categorie_coherent") & F.col("ticket_price_valide"), 1)).alias("prix_categorie_incoherents")
).collect()[0]

print(f"   - Prix valides : {validation_stats['prix_valides']:,}")
print(f"   - Prix invalides : {validation_stats['prix_invalides']:,}")
print(f"   - Prix cohérents avec catégorie : {validation_stats['prix_categorie_coherents']:,}")
print(f"   - Prix incohérents avec catégorie : {validation_stats['prix_categorie_incoherents']:,}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Normalisation des canaux d'achat
# MAGIC   
# MAGIC   **Valeurs acceptées** : `WEBSITE`, `MOBILE_APP`
# MAGIC   ________________________________________________

# COMMAND ----------

print("Analyse des canaux d'achat avant nettoyage :")
tickets_with_prices.groupBy("purchase_channel").count().orderBy(F.desc("count")).show(truncate=False)


# COMMAND ----------

# Normalisation des canaux d'achat
tickets_with_channels = tickets_with_prices.withColumn(
    "purchase_channel_clean",
    F.when(F.col("purchase_channel").isNull(), F.lit("UNKNOWN"))
     .otherwise(
         F.upper(F.trim(F.col("purchase_channel")))
     )
)

# COMMAND ----------

# Correction des formats
tickets_with_channels = tickets_with_channels.withColumn(
    "purchase_channel_fixed",
    F.when(F.col("purchase_channel_clean").isin(["WEBSITE", "MOBILE_APP"]), 
           F.col("purchase_channel_clean"))
     .when(F.col("purchase_channel_clean").rlike("^(WEB|WEBSITE_V2|ONLINE)$"), F.lit("WEBSITE"))
     .when(F.col("purchase_channel_clean").rlike("^(APP|PHONE)$"), F.lit("MOBILE_APP"))
     .otherwise(F.lit("INVALID"))
)

# COMMAND ----------

# Validation
tickets_with_channels = tickets_with_channels.withColumn(
    "purchase_channel_valide",
    F.col("purchase_channel_fixed").isin(["WEBSITE", "MOBILE_APP", "INVALID"])
)

print("Résultats de la normalisation des canaux :")
tickets_with_channels.groupBy("purchase_channel_fixed", "purchase_channel_valide").count().orderBy(F.desc("count")).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Validation des dates
# MAGIC   
# MAGIC **Règles** :
# MAGIC   1. `purchase_date` ne peut pas être après `scan_timestamp`
# MAGIC   2. `scan_timestamp` doit être raisonnable (dans la période du tournoi)

# COMMAND ----------

# Assurer le bon format des dates
tickets_with_dates = tickets_with_channels.withColumn(
    "purchase_date_ts",
    F.to_date(F.col("purchase_date"), "yyyy-MM-dd")
).withColumn(
    "scan_date_ts",
    F.to_date(F.col("scan_timestamp"))
).withColumn(
    "scan_timestamp_ts",
    F.to_timestamp(F.col("scan_timestamp"))
)

# Validation 1: purchase_date ne peut pas être après scan_timestamp
tickets_with_dates = tickets_with_dates.withColumn(
    "date_order_valide",
    F.col("purchase_date_ts") <= F.col("scan_date_ts")
)

# Validation 2: scan_timestamp dans une période raisonnable (décembre 2025)
tickets_with_dates = tickets_with_dates.withColumn(
    "scan_date_valide",
    (F.year(F.col("scan_timestamp_ts")) == 2025) &
    (F.month(F.col("scan_timestamp_ts")).between(12, 12))
)

# COMMAND ----------

print("Résultats de la validation des dates :")
date_stats = tickets_with_dates.agg(
    F.count(F.when(F.col("date_order_valide"), 1)).alias("ordre_dates_valide"),
    F.count(F.when(~F.col("date_order_valide"), 1)).alias("ordre_dates_invalide"),
    F.count(F.when(F.col("scan_date_valide"), 1)).alias("scan_date_valide"),
    F.count(F.when(~F.col("scan_date_valide"), 1)).alias("scan_date_invalide")
).collect()[0]

print(f"   - Ordre des dates valide : {date_stats['ordre_dates_valide']:,}")
print(f"   - Ordre des dates invalide : {date_stats['ordre_dates_invalide']:,}")
print(f"   - Date scan valide : {date_stats['scan_date_valide']:,}")
print(f"   - Date scan invalide : {date_stats['scan_date_invalide']:,}")


# COMMAND ----------

# Afficher des exemples de problèmes

print("Exemples de dates incohérentes :")
tickets_with_dates.filter(~F.col("date_order_valide")) \
    .select("ticket_id", "purchase_date", "scan_timestamp") \
    .limit(5).show(truncate=False)

# COMMAND ----------

# Si la date d'achat est antérieure à la date de scan, remplacez la date d'achat par la date de scan.

tickets_with_fixed_dates = tickets_with_dates.withColumn(
    "purchase_date_fixed",
    F.when(tickets_with_dates["date_order_valide"] == False, tickets_with_dates["scan_date_ts"])
    .otherwise(tickets_with_dates["purchase_date_ts"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Calcul du score de qualité global
# MAGIC ________________________________________________
# MAGIC

# COMMAND ----------

# Calculer un score de qualité pour chaque billet
tickets_with_quality = tickets_with_fixed_dates.withColumn(
    "quality_score",
    (F.when(F.col("ticket_id_valide"), 1).otherwise(0) +
     F.when(F.col("fan_id_valide"), 1).otherwise(0) +
     F.when(F.col("ticket_category_valide"), 1).otherwise(0) +
     F.when(F.col("ticket_price_valide"), 1).otherwise(0) +
     F.when(F.col("purchase_channel_valide"), 1).otherwise(0) ) / 6.0 * 100
)

# COMMAND ----------

tickets_with_quality = tickets_with_quality.withColumn(
    "completement_valide",
    (F.col("ticket_id_valide") &
     F.col("fan_id_valide") &
     F.col("ticket_category_valide") &
     F.col("ticket_price_valide") &
     F.col("purchase_channel_valide"))
)

# COMMAND ----------

print("Score de qualité des données :")
quality_stats = tickets_with_quality.agg(
    F.count("*").alias("total"),
    F.count(F.when(F.col("completement_valide"), 1)).alias("completement_valides"),
    F.avg("quality_score").alias("score_moyen"),
    F.min("quality_score").alias("score_min"),
    F.max("quality_score").alias("score_max")
).collect()[0]

print(f"   - Total billets : {quality_stats['total']:,}")
print(f"   - Billets complètement valides : {quality_stats['completement_valides']:,} ({quality_stats['completement_valides']/quality_stats['total']*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC # RÉSULTATS - QUALITÉ DONNÉES :
# MAGIC - Billets complètement valides : 99.88% 
# MAGIC - Seulement 1,740 billets problématiques sur 1.46M
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##  SAUVEGARDE TEMPORAIRE POUR NOTEBOOK SUIVANT
# MAGIC ________________________________________________

# COMMAND ----------

# Chemin de sauvegarde
silver_temp_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/tickets_cleaned_temp.delta"


# COMMAND ----------

print(f" Sauvegarde des données nettoyées...")

# Sélectionner les colonnes nécessaires pour le notebook 2
tickets_for_enrichment = tickets_with_fixed_dates.select(
    "ticket_id", "match_id", "stadium_id", "fan_id",
    "ticket_category_fixed", "ticket_price_mad", 
    "purchase_channel_fixed", "purchase_date_fixed",
    "entry_time", "entry_gate", "scan_status", 
    "scan_timestamp", "ticket_id_fixed", "ticket_id_valide",
    "fan_id_fixed", "fan_id_valide"
)

tickets_for_enrichment.write \
    .mode("overwrite") \
    .format("delta") \
    .save(silver_temp_path)

# Vérifier la sauvegarde
saved_count = spark.read.format("delta").load(silver_temp_path).count()

print(f"Sauvegarde terminée !")
print(f"{saved_count:,} billets sauvegardés")
