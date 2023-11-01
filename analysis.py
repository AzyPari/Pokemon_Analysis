from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, dense_rank, avg
from pyspark.sql import Window
import config.settings as settings

spark = SparkSession.builder \
		    .appName("PokemonAnalysis") \
		    .getOrCreate()

def load_data():
    data = spark.read \
		.csv(settings.DATA_PATH, 
		     header=True, 
		     inferSchema=True
		   )
    return data

def top_non_legendary(data):
    windowSpec = Window.orderBy(desc("Total"))
    non_legendary = data.filter(data.Legendary == False)
    ranked_data = non_legendary.withColumn("rank", dense_rank().over(windowSpec))
    return ranked_data.filter("rank <= 5").drop("rank")

def type_with_highest_avg_hp(data):
    type_avg_hp = data.groupBy("Type 1").agg(avg("HP").alias("Avg_HP"))
    windowSpec = Window.orderBy(desc("Avg_HP"))
    ranked_hp = type_avg_hp.withColumn("rank", dense_rank().over(windowSpec))
    return ranked_hp.filter("rank = 1").drop("rank")

def most_common_special_attack(data):
    sp_atk_count = data.groupBy("Sp. Atk").agg(count("Sp. Atk").alias("Count"))
    windowSpec = Window.orderBy(desc("Count"))
    ranked_sp_atk = sp_atk_count.withColumn("rank", dense_rank().over(windowSpec))
    return ranked_sp_atk.filter("rank = 1").drop("rank")

def main():
    data = load_data()
    print(top_non_legendary(data).show())
    print(type_with_highest_avg_hp(data).show())
    print(most_common_special_attack(data).show())
    
if __name__ == "__main__":
    main()
