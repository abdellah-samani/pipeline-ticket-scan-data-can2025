# Databricks notebook source
# MAGIC   %md
# MAGIC ## Notebook : 03_silver_tickets_enrichment
# MAGIC   - **Objectif** : Enrichir les match_id manquants et compléter les données
# MAGIC   - **Lecture** : Données nettoyées depuis Delta
# MAGIC   - **Écriture** : delta sur conteneur silver
# MAGIC   ________________________________________________________________________________
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Imports des librairies
# MAGIC   ________________________________________________

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration ADLS
# MAGIC   ________________________________________________

# COMMAND ----------

# Définir les informations d'accès ADLS
storage_account_name = "adlscan2025"
container_name_silver = "silver"
secret_scope = "secret-scope-can2025"
secret_name = "key-adls-can2025"

# Chemins
silver_temp_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/tickets_cleaned_temp.delta"
silver_final_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/ticket_scans_clean.delta"
matches_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/matches_clean.delta"
# Configurer les credentials
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=secret_scope, key=secret_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Chargement des données
# MAGIC   ________________________________________________
# MAGIC

# COMMAND ----------

# 1. Charger les billets nettoyés depuis Notebook 1
tickets_df = spark.read.format("delta").load(silver_temp_path)
print(f" Billets nettoyés chargés : {tickets_df.count():,}")

# 2. Charger les données des matchs
matches_df = spark.read.format("delta").load(matches_path)
print(f" Matchs chargés : {matches_df.count():,}")

# Afficher un aperçu
print("\n Aperçu billets nettoyés :")
tickets_df.limit(3).display()

print("\n Aperçu données matchs :")
matches_df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Phase 1 : Enrichissement des données

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrichissement des `match_id` manquants
# MAGIC   
# MAGIC   **Objectif**  
# MAGIC   Enrichir les `match_id` manquants dans les scans de billets en utilisant la table `matches` comme référence.
# MAGIC   
# MAGIC   **Logique métier**  
# MAGIC   Un scan de billet appartient à un match si :
# MAGIC   - Le `match_id` est NULL
# MAGIC   - Le scan a lieu dans le même stade
# MAGIC   - La date du scan correspond à la date du match
# MAGIC   
# MAGIC   **Hypothèse**  
# MAGIC   Un stade ne peut pas accueillir deux matchs le même jour.

# COMMAND ----------

# Préparer le DataFrame pour l'enrichissement
tickets_for_enrichment = tickets_df.select(
    "ticket_id", "match_id", "stadium_id", "scan_timestamp",
    "fan_id", "ticket_category_fixed", "ticket_price_mad", 
    "purchase_channel_fixed", "purchase_date_fixed", "entry_time",
    "entry_gate", "scan_status", "ticket_id_fixed", 
    "ticket_id_valide", "fan_id_fixed", "fan_id_valide"
).withColumnRenamed("ticket_category_fixed", "ticket_category") \
 .withColumnRenamed("purchase_channel_fixed", "purchase_channel")\
 .withColumnRenamed("purchase_date_fixed", "purchase_date")

# COMMAND ----------

# Identifier les billets avec match_id NULL
tickets_null = tickets_for_enrichment.filter(F.col("match_id").isNull())
print(f"Nombre de billets avec match_id NULL à corriger: {tickets_null.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1 Enrichissement des match_id NULL
# MAGIC   ________________________________________________

# COMMAND ----------

# Ajouter les colonnes de date
tickets_dated = tickets_for_enrichment.withColumn("scan_date", F.to_date("scan_timestamp"))
matches_dated = matches_df.withColumn("match_date_only", F.to_date("match_date"))

# Joindre avec alias
joined = tickets_dated.alias("t").join(
    matches_dated.alias("m"),
    (F.col("t.stadium_id") == F.col("m.stadium_id")) &
    (F.col("t.scan_date") == F.col("m.match_date_only")),
    "left"
)

# COMMAND ----------

# Utiliser coalesce pour remplacer les NULL
result_df = joined.withColumn(
    "match_id_final",
    F.coalesce(F.col("t.match_id"), F.col("m.match_id"))
)

# COMMAND ----------

# Sélectionner les colonnes finales
columns_to_select = []
for col_name in tickets_for_enrichment.columns:
    if col_name == "match_id":
        columns_to_select.append(F.col("match_id_final").alias("match_id"))
    else:
        columns_to_select.append(F.col(f"t.{col_name}"))

result_df = result_df.select(*columns_to_select).drop("scan_date")


# COMMAND ----------

# Afficher les résultats
null_avant = tickets_for_enrichment.filter(F.col("match_id").isNull()).count()
null_apres = result_df.filter(F.col("match_id").isNull()).count()

print(f"Résultats de l'enrichissement :")
print(f"   - Match_id NULL avant : {null_avant:,}")
print(f"   - Match_id NULL après : {null_apres:,}")
print(f"   - Match_id corrigés : {null_avant - null_apres:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.2 Correction des stadium_id NULL
# MAGIC   ________________________________________________
# MAGIC

# COMMAND ----------

# Créer une table de correspondance match_id → stadium_id
stadium_lookup = matches_df.select(
    F.col("match_id").alias("lookup_match_id"),
    F.col("stadium_id").alias("match_stadium_id")
).distinct()

# COMMAND ----------

# Joindre et mettre à jour les stadium_id
final_df = result_df.alias("r").join(
    stadium_lookup.alias("l"),
    F.col("r.match_id") == F.col("l.lookup_match_id"),
    "left"
).withColumn(
    "stadium_id",
    F.coalesce(F.col("r.stadium_id"), F.col("l.match_stadium_id"))
).select(
    "ticket_id", "match_id", "stadium_id", "fan_id", 
    "ticket_category", "ticket_price_mad", "purchase_channel",
    "purchase_date", "entry_time", "entry_gate", "scan_status",
    "scan_timestamp", "ticket_id_fixed", "ticket_id_valide",
    "fan_id_fixed", "fan_id_valide"
)

# COMMAND ----------

# Afficher les statistiques
stadium_null_before = result_df.filter(F.col("stadium_id").isNull()).count()
stadium_null = final_df.filter(F.col("stadium_id").isNull()).count()
print(f"Stadium_id NULL avant correction : {stadium_null_before:,}")
print(f"Stadium_id NULL après correction : {stadium_null:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2 : Validation finale
# MAGIC  ________________________________________________________________

# COMMAND ----------

# Statistiques basiques
print(f"\n STATISTIQUES FINALES :")
print(f"   - Total billets : {final_df.count():,}")
print(f"   - Match_id NULL : {final_df.filter(F.col('match_id').isNull()).count():,}")
print(f"   - Stadium_id NULL : {final_df.filter(F.col('stadium_id').isNull()).count():,}")

# COMMAND ----------

# Qualité des données
quality_stats = final_df.agg(
    F.count(F.when(F.col("ticket_category").isin(["CATEGORY_1", "CATEGORY_2", "CATEGORY_3"]), 1)).alias("categories_ok"),
    F.count(F.when(F.col("ticket_price_mad") >= 0, 1)).alias("prix_ok"),
    F.count(F.when(F.col("purchase_channel").isin(["WEBSITE", "MOBILE_APP"]), 1)).alias("canaux_ok")
).collect()[0]

print(f"\n QUALITÉ DES DONNÉES :")
print(f"   - Catégories valides : {quality_stats['categories_ok']:,} ({quality_stats['categories_ok']/final_df.count()*100:.1f}%)")
print(f"   - Prix valides : {quality_stats['prix_ok']:,} ({quality_stats['prix_ok']/final_df.count()*100:.1f}%)")
print(f"   - Canaux valides : {quality_stats['canaux_ok']:,} ({quality_stats['canaux_ok']/final_df.count()*100:.1f}%)")


# COMMAND ----------

# Échantillon
print("\n ÉCHANTILLON FINAL :")
final_df.limit(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Préparation finale des colonnes
# MAGIC ________________________________________________

# COMMAND ----------

print("Préparation finale des colonnes...")

# Solution hybride : Garder les deux mais rendre clair
final_for_silver = final_df.select(
    # Identifiants originaux (pour audit)
    F.col("ticket_id").alias("ticket_id_original"),
    F.col("fan_id").alias("fan_id_original"),
    
    # Identifiants nettoyés (pour utilisation)
    F.col("ticket_id_fixed").alias("ticket_id"),
    F.col("fan_id_fixed").alias("fan_id"),
    
    # Flags de validation
    F.col("ticket_id_valide").alias("is_ticket_id_valid"),
    F.col("fan_id_valide").alias("is_fan_id_valid"),
    
    # Autres colonnes
    "match_id", "stadium_id", "ticket_category", 
    "ticket_price_mad", "purchase_channel", "purchase_date",
    "entry_time", "entry_gate", "scan_status", "scan_timestamp"
)

print("Colonnes préparées :")
print("   - Originaux préservés (audit)")
print("   - Nettoyés utilisés (analysis)")
print("   - Flags gardés (quality)")

# COMMAND ----------

silver_final_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/ticket_scans_clean"

final_for_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .save(silver_final_path)

print(f"Données sauvegardées : {silver_final_path}")


# COMMAND ----------

# Supprimer les données temporaires
dbutils.fs.rm(silver_temp_path, recurse=True)