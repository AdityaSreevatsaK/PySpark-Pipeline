from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("GettingStarted") \
    .getOrCreate()

'''
Here's a breakdown of the code:

master("local"): Indicates that the Spark application should execute locally on your machine using a single thread. 
This setup is suitable for basic development and testing without requiring a cluster. 

You can also configure it with:
- "local[3]": Allocates 3 threads for the application, which may run across up to 3 cores (if available).
- "local[*]": Uses all available cores on your local machine, with one thread per core.
- "spark://HOST:PORT": Connects to a standalone Spark cluster at the specified host and port.
- "yarn": Deploys the application on a Hadoop YARN cluster.
- appName("GettingStarted"): Sets a user-friendly name for your application, aiding in the identification and 
    monitoring of your Spark job.
- getOrCreate(): This method finalizes the configuration and either retrieves an existing SparkSession with the same 
    settings or creates a new one if none exists.

With your SparkSession ready, you’re now prepared to start processing data effectively with Spark.
'''

# Define a simple list of numbers as input data
nums = [1, 2, 3, 4, 5]

# Convert the list into an RDD (Resilient Distributed Dataset)
nums_rdd = spark.sparkContext.parallelize(nums)

# Retrieve and print all elements from the RDD collection
print("All elements in the RDD:", nums_rdd.collect())

# Stop SparkSession to release resources
spark.stop()
