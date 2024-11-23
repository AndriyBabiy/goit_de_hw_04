#------------------------------
#   Task 1 - Setup code
#------------------------------
# from pyspark.sql import SparkSession

# # Create Spark session
# spark = SparkSession.builder \
#     .master("local[*]") \
#     .config("spark.sql.shuffle.partitions", "2") \
#     .appName("MyGoitSparkSandbox") \
#     .getOrCreate()

# # Load dataset
# nuek_df = spark.read \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv('./data/nuek-vuh3.csv')

# nuek_repart = nuek_df.repartition(2)

# nuek_processed = nuek_repart \
#     .where("final_priority < 3") \
#     .select("unit_id", "final_priority") \
#     .groupBy("unit_id") \
#     .count()

# # Ось ТУТ додано рядок
# nuek_processed = nuek_processed.where("count>2")

# nuek_processed.collect()

# input("Press Enter to continue...5")

# # Close Spark session
# spark.stop()

#------------------------------
#   Task 2 - Intermediate collect action
# The addition of the intermediary collect function resulted in 3 additiional jobs.
#------------------------------
# from pyspark.sql import SparkSession

# # Створюємо сесію Spark
# spark = SparkSession.builder \
#     .master("local[*]") \
#     .config("spark.sql.shuffle.partitions", "2") \
#     .appName("MyGoitSparkSandbox") \
#     .getOrCreate()

# # Завантажуємо датасет
# nuek_df = spark.read \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv('./data/nuek-vuh3.csv')

# nuek_repart = nuek_df.repartition(2)

# nuek_processed = nuek_repart \
#     .where("final_priority < 3") \
#     .select("unit_id", "final_priority") \
#     .groupBy("unit_id") \
#     .count()
    
# # Проміжний action: collect
# nuek_processed.collect()

# # Ось ТУТ додано рядок
# nuek_processed = nuek_processed.where("count>2")

# nuek_processed.collect()

# input("Press Enter to continue...5")

# # Закриваємо сесію Spark
# spark.stop()

#------------------------------
#   Task 3 - Using a cache function
# In comparison to the previous operation there is one less job as one of the workers could use the cached information rather than having to collect the information separately.
#------------------------------

from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox") \
    .getOrCreate()

# Завантажуємо датасет
nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('./data/nuek-vuh3.csv')

nuek_repart = nuek_df.repartition(2)

nuek_processed_cached = nuek_repart \
    .where("final_priority < 3") \
    .select("unit_id", "final_priority") \
    .groupBy("unit_id") \
    .count() \
    .cache()  # Додано функцію cache

# Проміжний action: collect
nuek_processed_cached.collect()

# Ось ТУТ додано рядок
nuek_processed = nuek_processed_cached.where("count>2")

nuek_processed.collect()

input("Press Enter to continue...5")

# Звільняємо пям'ять від Dataframe
nuek_processed_cached.unpersist()

# Закриваємо сесію Spark
spark.stop()