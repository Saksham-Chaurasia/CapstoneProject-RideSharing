from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

def spark_transform():
    spark = SparkSession \
    .builder \
    .appName("Ride_Sharing_Service") \
    .master("local[1]") \
    .getOrCreate()
    

    
    driver_data = spark.read.csv("hdfs://localhost:9000/data/driver_data",sep=',',header=True,inferSchema=True)
    
    customer_data = spark.read.csv("hdfs://localhost:9000/data/customer_data",sep=',',header=True,inferSchema=True)
    
    ride_data = spark.read.csv("hdfs://localhost:9000/data/ride_data",sep=',',header=True,inferSchema=True)
    
    # Transforming and cleansing the data 
    driver = driver_data.withColumn("driver_id", expr("substring(driver_id, 10, length(driver_id))"))

    customer = customer_data.withColumn("customer_id", expr("substring(customer_id, 7, length(customer_id))"))

    ride = ride_data.withColumn("id", expr("substring(id, 3, length(id))")) \
    .withColumn("customer_id",expr("substring(customer_id,7,length(customer_id))")) \
    .withColumn("driver_id",expr("substring(driver_id,10,length(driver_id))"))
    
    
    
    driver.write.mode("overwrite").csv("hdfs://localhost:9000/data/driver", header=True)
    customer.write.mode("overwrite").csv("hdfs://localhost:9000/data/customer", header=True)
    ride.write.mode("overwrite").csv("hdfs://localhost:9000/data/ride", header=True, timestampFormat="yyyy-MM-dd' 'HH:mm:ss")
    
    # driver.show()
    # customer.show()
    # ride.show()


    