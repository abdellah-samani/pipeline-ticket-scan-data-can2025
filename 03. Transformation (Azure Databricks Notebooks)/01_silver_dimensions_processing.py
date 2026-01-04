# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook: 01_silver_dimensions_processing
# MAGIC  - **Objectif**: Nettoyer les 4 petits datasets pour Silver Layer
# MAGIC  - **Sortie**: Delta files dans Silver container
# MAGIC  _________________
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. IMPORTS
# MAGIC _______________

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CONFIGURATION ADLS
# MAGIC _______________

# COMMAND ----------

storage_account = "adlscan2025"
bronze_container = "bronze"
silver_container = "silver"

# Set credentials
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="secret-scope-can2025", key="key-adls-can2025")
)

# Paths
bronze_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/"
silver_path = f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. CHARGER TOUS LES JEUX DE DONNÉES
# MAGIC _____________

# COMMAND ----------

stadiums = spark.read.parquet(bronze_path + "/infrastructure_data/stadiums_raw.parquet")
teams = spark.read.parquet(bronze_path + "/competition_data/teams_raw.parquet")
cities = spark.read.parquet(bronze_path + "/infrastructure_data/cities_raw.parquet")
matches = spark.read.parquet(bronze_path + "/competition_data/matches_raw.parquet")

print("Données chargées")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. NETTOYER LES DONNÉES DES STADES
# MAGIC _______________

# COMMAND ----------

stadiums_clean = (stadiums
    .withColumn("stadium_name", F.initcap(F.trim("stadium_name")))
    .withColumn("city_code", F.upper(F.trim("city_code")))
    .withColumn("full_address", F.trim("full_address"))
    .withColumn("pitch_type", F.upper(F.trim("pitch_type")))
    .withColumn("contact_phone", F.regexp_replace("contact_phone", "\\s+", ""))
    .withColumn("email", F.lower(F.trim("email")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  5. NETTOYER LES DONNÉES DES ÉQUIPES
# MAGIC _________________

# COMMAND ----------

teams_clean = (teams
    .withColumn("team_name", F.initcap(F.trim("team_name")))
    .withColumn("team_code", F.upper(F.trim("team_code")))
    .withColumn("group", F.upper(F.trim("group")))
    .withColumn("federation", F.upper(F.trim("federation")))
    .withColumn("coach_name", F.trim("coach_name"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. NETTOYER LES DONNÉES DES VILLES
# MAGIC __________________

# COMMAND ----------

cities_clean = (cities
    .withColumn("city_name", F.initcap(F.trim("city_name")))
    .withColumn("city_code", F.upper(F.trim("city_code")))
    .withColumn("region", F.trim("region"))
    .withColumn("airport_code", F.upper(F.trim("airport_code")))
    .withColumn("climate_type", F.trim("climate_type"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. NETTOYER LES DONNÉES DES MATCHS 
# MAGIC (Correction du bug d'heure + nettoyage de base)
# MAGIC ________________________

# COMMAND ----------

matches_clean = (matches
    # Corriger la colonne de l'heure (extraire le format HH:MM:SS du format étrange)
    .withColumn("match_time_fixed", F.regexp_extract("match_time", r"(\d{2}:\d{2}:\d{2})", 0))
    
    # Nettoyer les champs de texte
    .withColumn("stage", F.trim("stage"))
    .withColumn("group_code", F.upper(F.trim("group_code")))
    .withColumn("stadium_id", F.upper(F.trim("stadium_id")))
    .withColumn("home_team_code", F.upper(F.trim("home_team_code")))
    .withColumn("away_team_code", F.upper(F.trim("away_team_code")))
    .withColumn("weather_notes", F.trim("weather_notes"))
    
    # Créez datetime correcte
    .withColumn("match_datetime", F.to_timestamp(F.concat("match_date", F.lit(" "), "match_time_fixed")))
)

# COMMAND ----------

# Sélectionnez les colonnes finales
matches_final = matches_clean.select(
    "match_id",
    "match_date",
    F.col("match_time_fixed").alias("match_time"),
    "match_datetime",
    "stage",
    "group_code",
    "stadium_id",
    "home_team_code",
    "away_team_code",
    "weather_notes"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. ENREGISTREZ TOUT COMME FICHIERS DELTA
# MAGIC ___________

# COMMAND ----------

print("Sauvegarde des fichiers Delta...")

stadiums_clean.write.format("delta").mode("overwrite").save(silver_path + "stadiums_clean.delta")
teams_clean.write.format("delta").mode("overwrite").save(silver_path + "teams_clean.delta")
cities_clean.write.format("delta").mode("overwrite").save(silver_path + "cities_clean.delta")
matches_final.write.format("delta").mode("overwrite").save(silver_path + "matches_clean.delta")

print("Tous les fichiers Delta sauvegardés dans Silver!")
