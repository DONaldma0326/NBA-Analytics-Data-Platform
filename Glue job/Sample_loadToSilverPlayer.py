
args = getResolvedOptions(sys.argv, ['JOB_NAME','season','silver_path','gold_path'])

season = args["season"]
silver_path = args["silver_path"]
gold_path = args["gold_path"]


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job.init(args['JOB_NAME'], args)


df = spark.read.parquet(silver_path)
season_df = df[df['season']==season]


# ---- Identify Team Change Players ----
team_change_df = season_df.filter(col('team').isin(['2TM', '3TM', '4TM']))
normal_df = season_df.filter(~col('team').isin(['2TM', '3TM', '4TM']))

# ---- Extract Number of Teams ----
team_change_df = team_change_df.withColumn('team_count', regexp_extract(col('team'), r'(\d)', 1).cast('int'))
normal_df = normal_df.withColumn('team_count', lit(1))

# ---- Mark players with multiple teams ----
multi_team_player_ids = [row['player_id'] for row in team_change_df.select('player_id').distinct().collect()]
from pyspark.sql.functions import when
normal_df = normal_df.withColumn('changed_team', when(col('player_id').isin(multi_team_player_ids), 1).otherwise(0))

# ---- Drop 2TM, 3TM, 4TM rows ----
final_gold_df = normal_df

# ---- Filter Only Valid Teams ----
valid_teams = [
    'SAC','HOU','MIA','TOR','MEM','ATL','NOP','PHO','CLE','UTA','MIL',
    'NYK','POR','LAL','WAS','CHO','ORL','PHI','SAS','OKC','LAC','MIN',
    'BOS','IND','DEN','GSW','CHI','DAL','BRK','DET'
]
final_gold_df = final_gold_df.filter(col('team').isin(valid_teams))

final_gold_df = final_gold_df.withColumn("player_id", col("player_id").cast("string")) \
       .withColumn("player", col("player").cast("string")) \
       .withColumn("x3p", col("x3p").cast("double")) \
       .withColumn("pts", col("pts").cast("double")) \
       .withColumn("fg", col("fg").cast("double")) \
       .withColumn("trb", col("trb").cast("double")) \
       .withColumn("fga", col("fga").cast("double")) \
       .withColumn("ast", col("ast").cast("double")) \
       .withColumn("fg_percent", col("fg_percent").cast("double")) \
       .withColumn("stl", col("stl").cast("double")) \
       .withColumn("x3pa", col("x3pa").cast("double")) \
       .withColumn("fta", col("fta").cast("double")) \
       .withColumn("blk", col("blk").cast("double")) \
       .withColumn("ft_percent", col("ft_percent").cast("double")) \
       .withColumn("g", col("g").cast("integer")) \
       .withColumn("x3p_percent", col("x3p_percent").cast("double")) \
       .withColumn("x2pa", col("x2pa").cast("double")) \
       .withColumn("x2p", col("x2p").cast("double")) \
       .withColumn("tov", col("tov").cast("double")) \
       .withColumn("drb", col("drb").cast("double")) \
       .withColumn("mp", col("mp").cast("double")) \
       .withColumn("age", col("age").cast("double")) \
       .withColumn("gs", col("gs").cast("integer")) \
       .withColumn("x2p_percent", col("x2p_percent").cast("double")) \
       .withColumn("pos", col("pos").cast("string")) \
       .withColumn("ft", col("ft").cast("double")) \
       .withColumn("orb", col("orb").cast("double")) \
       .withColumn("e_fg_percent", col("e_fg_percent").cast("double")) \
       .withColumn("team_count", col("team_count").cast("integer")) \
       .withColumn("changed_team", col("changed_team").cast("integer"))




row_count = final_gold_df.count()
if row_count <= 0:
    raise ValueError("Sanity check failed: final_gold_df is empty!")
else:
    print(f"Sanity check passed: final_gold_df has {row_count} rows.")

final_gold_df.write.mode("overwrite").partitionBy("season", "team").parquet(gold_path)

job.commit()