from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WordCounterApplication") \
    .getOrCreate()

hdfs_path = 'Data/Word Counter.txt'
rdd = spark.sparkContext.textFile(hdfs_path)
words = rdd.flatMap(lambda line: line.split(' '))

word_map = words.map(lambda word: (word, 1))

word_count = word_map.reduceByKey(lambda a, b: a + b)
inline_word_count = rdd.flatMap(lambda line: line.split(' ')) \
                        .map(lambda word: (word, 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .collect()

print(word_count.collect())
print(inline_word_count)

spark.stop()

# Output
# [('Python', 4), ('Java', 1), ('SQL', 3), ('PostgreSQL', 1), ('Tensorflow', 1), ('Keras', 1), ('PyTorch', 1),
# ('MySQL', 1)]
