# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook: 04_gold_star_schema_creation
# MAGIC - **Objectif**: Créer la couche Gold avec un schéma en étoile
# MAGIC - **Entrée**: Données nettoyées depuis Silver Layer
# MAGIC - **Sortie**: Table de faits et dimensions dans Gold container
# MAGIC - **Architecture**: Schéma en étoile (Star Schema)
# MAGIC ______________

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports
# MAGIC ________________________________________________

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration ADLS
# MAGIC ________________________________________________

# COMMAND ----------

# Définir les informations d'accès ADLS
storage_account_name = "adlscan2025"
container_name_silver = "silver"
container_name_gold = "gold"
secret_scope = "secret-scope-can2025"
secret_name = "key-adls-can2025"

# Chemins Silver
silver_tickets_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/ticket_scans_clean"
silver_matches_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/matches_clean.delta"
silver_stadiums_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/stadiums_clean.delta"
silver_teams_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/teams_clean.delta"
silver_cities_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/cities_clean.delta"

# Chemins Gold
gold_base_path = f"abfss://{container_name_gold}@{storage_account_name}.dfs.core.windows.net/"

# Configurer les credentials
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=secret_scope, key=secret_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Chargement des données Silver
# MAGIC ________________________________________________

# COMMAND ----------

print("Chargement des données depuis Silver Layer...")

# Charger les données
tickets_df = spark.read.format("delta").load(silver_tickets_path)
matches_df = spark.read.format("delta").load(silver_matches_path)
stadiums_df = spark.read.format("delta").load(silver_stadiums_path)
teams_df = spark.read.format("delta").load(silver_teams_path)
cities_df = spark.read.format("delta").load(silver_cities_path)

# Afficher les statistiques
print(f"\nTaille des datasets:")
print(f"  - Tickets : {tickets_df.count():,} lignes")
print(f"  - Matchs : {matches_df.count():,} lignes")
print(f"  - Stades : {stadiums_df.count():,} lignes")
print(f"  - Équipes : {teams_df.count():,} lignes")
print(f"  - Villes : {cities_df.count():,} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dimensions dénormalisées (Schéma en étoile)
# MAGIC ________________________________________________

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 DIM_STADIUM (avec infos ville incluses)
# MAGIC ________________________________________________

# COMMAND ----------


dim_stadium = stadiums_df.alias("s").join(
    cities_df.alias("c"), 
    F.col("s.city_code") == F.col("c.city_code"), 
    "left"
).select(
    F.col("s.stadium_id").alias("stadium_key"),
    "s.stadium_name",
    "s.city_code",
    F.col("c.city_name").alias("city_name"),
    F.col("c.region").alias("region"),
    "s.full_address",
    "s.seating_capacity",  
    "s.pitch_type",
    F.current_timestamp().alias("load_ts")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 DIM_TEAM
# MAGIC ____________________

# COMMAND ----------

dim_team = teams_df.select(
    F.col("team_code").alias("team_key"),
    "team_name",
    F.col("group").alias("team_group"), 
    "federation",
    "coach_name",
    F.current_timestamp().alias("load_ts")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 DIM_MATCH (complètement dénormalisé)

# COMMAND ----------

dim_match = matches_df.alias("m").join(
    stadiums_df.alias("s"), 
    F.col("m.stadium_id") == F.col("s.stadium_id"), 
    "left"
).join(
    cities_df.alias("c"), 
    F.col("s.city_code") == F.col("c.city_code"), 
    "left"
).join(
    teams_df.alias("home_team"),
    F.col("m.home_team_code") == F.col("home_team.team_code"),
    "left"
).join(
    teams_df.alias("away_team"),
    F.col("m.away_team_code") == F.col("away_team.team_code"),
    "left"
).select(
    F.col("m.match_id").alias("match_key"),
    "m.match_date",
    "m.match_time",
    "m.match_datetime",
    "m.stage",
    F.col("m.group_code").alias("match_group"),
    
    "s.stadium_id",
    "s.stadium_name",
    F.col("home_team.team_name").alias("home_team_name"),  
    F.col("away_team.team_name").alias("away_team_name"),
    
    "m.weather_notes",
    F.current_timestamp().alias("load_ts")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 DIM_TIME

# COMMAND ----------

scan_dates = tickets_df.select(
    F.to_date("scan_timestamp").alias("event_date")
).filter(F.col("event_date").isNotNull()).distinct()

match_dates = matches_df.select(
    F.to_date("match_datetime").alias("event_date")
).filter(F.col("event_date").isNotNull()).distinct()

all_dates = scan_dates.union(match_dates).distinct()

dim_time = all_dates.filter(F.col("event_date").isNotNull()).select(
    F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
    "event_date",
    F.year("event_date").alias("year"),
    F.month("event_date").alias("month"),
    F.date_format("event_date", "MMMM").alias("month_name"),
    F.dayofmonth("event_date").alias("day"),
    F.when(F.dayofweek("event_date").isin([1, 7]), "Weekend")
     .otherwise("Weekday").alias("day_type"),
    F.current_timestamp().alias("load_ts")
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 DIM_FAN
# MAGIC ___________

# COMMAND ----------

#Extraire les fans uniques (sauf N/A)
identified_fans = tickets_df.filter(
    (F.col("fan_id") != "N/A") & 
    (F.col("is_fan_id_valid") == True)
).select("fan_id").distinct()

print(f"\n   - Fans identifiés uniques: {identified_fans.count():,}")

# COMMAND ----------

# Créer dim_fan avec clés entières
dim_fan = identified_fans.withColumn(
    "fan_key", 
    F.row_number().over(Window.orderBy("fan_id"))
).select(
    F.col("fan_key").cast("integer").alias("fan_key"),
    F.col("fan_id"),
    F.lit("Regular").alias("fan_category"),  # Tous "Regular" car format FAN_XXXXXX
    F.lit("Identified").alias("fan_type"),
    F.current_timestamp().alias("load_ts")
)

# COMMAND ----------

# Ajouter fan anonyme (N/A)
anonymous_fan = spark.createDataFrame(
    [(0, "N/A", "Unidentified", "Undentified")],
    ["fan_key", "fan_id", "fan_category", "fan_type"]
).withColumn("load_ts", F.current_timestamp())

dim_fan = dim_fan.unionByName(anonymous_fan)

print(f"\n dim_fan créée: {dim_fan.count():,} entrées")
print(f"   - Fans identifiés (FAN_XXXXXX): {dim_fan.filter(F.col('fan_key') > 0).count():,}")
print(f"   - Fan anonyme (N/A): {dim_fan.filter(F.col('fan_key') == 0).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Table de faits (FACT_TICKET_SCAN)

# COMMAND ----------

# Mapping pour clés d'équipe
team_mapping = matches_df.select(
    F.col("match_id").alias("match_key"),
    F.col("home_team_code").alias("home_team_key"),
    F.col("away_team_code").alias("away_team_key")
).distinct()

# COMMAND ----------

print("Création des clés de substitution entières...")

window_fact = Window.orderBy("ticket_id", "scan_timestamp")

fact_ticket_scan = tickets_df.alias("t").join(
    team_mapping.alias("tm"),
    F.col("t.match_id") == F.col("tm.match_key"),
    "left"
).join(
    dim_fan.alias("df"),
    F.when(
        F.col("t.fan_id") == "N/A",
        F.lit("N/A")
    ).otherwise(F.col("t.fan_id")) == F.col("df.fan_id"),
    "left"
).withColumn(
    "date_key",
    F.date_format(F.to_date("t.scan_timestamp"), "yyyyMMdd").cast("int")
).withColumn(
    "row_num", F.row_number().over(window_fact)
).select(
    # clé de substitution
    F.col("row_num").cast("bigint").alias("ticket_scan_key"),
    # 6 Clés étrangères
    F.coalesce(F.col("t.match_id"), F.lit("UNKNOWN")).alias("match_key"),
    F.coalesce(F.col("t.stadium_id"), F.lit("UNKNOWN")).alias("stadium_key"),
    F.coalesce(F.col("df.fan_key"), F.lit(0)).alias("fan_key"),
    F.col("date_key"),
    F.coalesce(F.col("tm.home_team_key"), F.lit("UNKNOWN")).alias("home_team_key"),
    F.coalesce(F.col("tm.away_team_key"), F.lit("UNKNOWN")).alias("away_team_key"),
    
    "t.ticket_id",
    "t.ticket_category",
    "t.ticket_price_mad",
    "t.purchase_channel",
    "t.scan_status",
    "t.scan_timestamp",
    "t.entry_gate",
    
    
    # Partition
    F.date_format(F.to_date("t.scan_timestamp"), "yyyy-MM-dd").alias("scan_day"),
    
    F.current_timestamp().alias("load_ts")
)

# Verify uniqueness
unique_count = fact_ticket_scan.select("ticket_scan_key").distinct().count()
total_count = fact_ticket_scan.count()
print(f"\n Vérification unicité des clés:")
print(f"   - Clés uniques: {unique_count:,}")
print(f"   - Total enregistrements: {total_count:,}")
print(f"   - Tous uniques: {'OUI' if unique_count == total_count else 'NON'}")

# COMMAND ----------

fact_ticket_scan.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sauvegarde dans GOLD
# MAGIC ____________________

# COMMAND ----------

dim_stadium.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"{gold_base_path}dim_stadium")
print(f"  ✓ dim_stadium          → {gold_base_path}dim_stadium")

dim_team.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"{gold_base_path}dim_team")
print(f"  ✓ dim_team             → {gold_base_path}dim_team")

dim_match.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"{gold_base_path}dim_match")
print(f"  ✓ dim_match            → {gold_base_path}dim_match")

dim_time.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"{gold_base_path}dim_time")
print(f"  ✓ dim_time             → {gold_base_path}dim_time")

dim_fan.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"{gold_base_path}dim_fan")
print(f"  ✓ dim_fan              → {gold_base_path}dim_fan")

fact_ticket_scan.write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy("scan_day").save(f"{gold_base_path}fact_ticket_scan")
print(f"  ✓ fact_ticket_scan     → {gold_base_path}fact_ticket_scan")

print("\nToutes les tables sauvegardées!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Enregistrez les tables dans UNITY CATALOG
# MAGIC _____________________

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dimensions
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_stadium
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@adlscan2025.dfs.core.windows.net/dim_stadium';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_team
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@adlscan2025.dfs.core.windows.net/dim_team';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_match
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@adlscan2025.dfs.core.windows.net/dim_match';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_time
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@adlscan2025.dfs.core.windows.net/dim_time';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_fan
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@adlscan2025.dfs.core.windows.net/dim_fan';
# MAGIC
# MAGIC -- Fact table
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_ticket_scan
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@adlscan2025.dfs.core.windows.net/fact_ticket_scan';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN gold;

# COMMAND ----------

dim_fan.printSchema()
fact_ticket_scan.printSchema()
dim_team.printSchema()
dim_stadium.printSchema()
dim_match.printSchema()
dim_time.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fin du notebook