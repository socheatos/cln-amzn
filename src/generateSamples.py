from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("dataSampler") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.files.maxPartitionBytes", "256m") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()


# spark = SparkSession.builder \
#     .appName("LargeJSONProcessing") \
#     .master("local[*]") \  # Use all cores on your M1 Pro
#     .config("spark.driver.memory", "12g") \  # Allocate more memory
#     .config("spark.executor.memory", "12g") \
#     .config("spark.sql.files.maxPartitionBytes", "256m") \  # Control partition size
#     .config("spark.sql.shuffle.partitions", "200") \  # For better parallelism
#     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \  # ARM optimization
#     .config("spark.sql.adaptive.enabled", "true") \  # Adaptive query execution
#     .getOrCreate()

# spark.debug.maxToStringFields=100
# get rid of truncated warnign 
spark.conf.set('spark.sql.caseSensitive', True) 
# gets rid of the COLUMN ALREADY EXISTS error
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
spark.catalog.clearCache()

PATH = './dataset/'
REVIEW_DATA = 'Clothing_Shoes_and_Jewelry.json'
PRODUCT_DATA = 'meta_Clothing_Shoes_and_Jewelry.json'


reviews = spark.read.json(PATH+REVIEW_DATA)
products = spark.read.json(PATH+PRODUCT_DATA)
print(f"Total reviews: {reviews.count():,}")
print(f"Total products: {products.count():,}")

print('sampling products...')
products = products.sample(fraction=0.001, seed = 42)
sampled_IDs = products.select('asin').persist()
print(f"Total sampled product IDs: {sampled_IDs.count():,}")

reviews = reviews.join(sampled_IDs,
                       'asin',
                       'inner')
print(f"Total matching reviews: {reviews.count():,}")


print('Done sampling!Checking ID counts...')
print(f"Unique product ID count:{sampled_IDs.distinct().count():,}")
print(f"Unique review ID count:{reviews.select('asin').distinct().count():,}")

# Write sampled data as json


products.repartition(1).write.json(PATH+"sample/meta_Clothing_Shoes_and_Jewelry") 
reviews.repartition(1).write.json(PATH+"sample/Clothing_Shoes_and_Jewelry") 




sampled_IDs.unpersist()
spark.stop()