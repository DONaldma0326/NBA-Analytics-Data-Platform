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
final_gold_df = df[df['season']==season]

final_gold_df = final_gold_df.withColumn("team", col("team").cast("string")) \
       .withColumn("w", col("w").cast("integer")) \
       .withColumn("n_rtg", col("n_rtg").cast("double")) \
       .withColumn("pace", col("pace").cast("double")) \
       .withColumn("playoffs", col("playoffs").cast("boolean")) \
       .withColumn("orb_percent", col("orb_percent").cast("double")) \
       .withColumn("f_tr", col("f_tr").cast("double")) \
       .withColumn("drb_percent", col("drb_percent").cast("double")) \
       .withColumn("opp_tov_percent", col("opp_tov_percent").cast("double")) \
       .withColumn("abbreviation", col("abbreviation").cast("string")) \
       .withColumn("ft_fga", col("ft_fga").cast("double")) \
       .withColumn("tov_percent", col("tov_percent").cast("double")) \
       .withColumn("o_rtg", col("o_rtg").cast("double")) \
       .withColumn("d_rtg", col("d_rtg").cast("double")) \
       .withColumn("age", col("age").cast("double")) \
       .withColumn("ts_percent", col("ts_percent").cast("double")) \
       .withColumn("arena", col("arena").cast("string")) \
       .withColumn("opp_e_fg_percent", col("opp_e_fg_percent").cast("double")) \
       .withColumn("l", col("l").cast("integer")) \
       .withColumn("x3p_ar", col("x3p_ar").cast("double")) \
       .withColumn("e_fg_percent", col("e_fg_percent").cast("double")) \
       .withColumn("opp_ft_fga", col("opp_ft_fga").cast("double")) \
       .withColumn("ast_per_game", col("ast_per_game").cast("double")) \
       .withColumn("x3p_per_game", col("x3p_per_game").cast("double")) \
       .withColumn("orb_per_game", col("orb_per_game").cast("double")) \
       .withColumn("fta_per_game", col("fta_per_game").cast("double")) \
       .withColumn("trb_per_game", col("trb_per_game").cast("double")) \
       .withColumn("fg_percent", col("fg_percent").cast("double")) \
       .withColumn("x3pa_per_game", col("x3pa_per_game").cast("double")) \
       .withColumn("stl_per_game", col("stl_per_game").cast("double")) \
       .withColumn("ft_percent", col("ft_percent").cast("double")) \
       .withColumn("drb_per_game", col("drb_per_game").cast("double")) \
       .withColumn("mp_per_game", col("mp_per_game").cast("double")) \
       .withColumn("g", col("g").cast("integer")) \
       .withColumn("x2pa_per_game", col("x2pa_per_game").cast("double")) \
       .withColumn("x3p_percent", col("x3p_percent").cast("double")) \
       .withColumn("pts_per_game", col("pts_per_game").cast("double")) \
       .withColumn("ft_per_game", col("ft_per_game").cast("double")) \
       .withColumn("tov_per_game", col("tov_per_game").cast("double")) \
       .withColumn("pf_per_game", col("pf_per_game").cast("double")) \
       .withColumn("fg_per_game", col("fg_per_game").cast("double")) \
       .withColumn("blk_per_game", col("blk_per_game").cast("double")) \
       .withColumn("x2p_percent", col("x2p_percent").cast("double")) \
       .withColumn("fga_per_game", col("fga_per_game").cast("double")) \
       .withColumn("x2p_per_game", col("x2p_per_game").cast("double"))


row_count = final_gold_df.count()
if row_count <= 0:
    raise ValueError("Sanity check failed: final_gold_df is empty!")
else:
    print(f"Sanity check passed: final_gold_df has {row_count} rows.")

final_gold_df.write.mode("overwrite").partitionBy("season").parquet(gold_path)

job.commit()