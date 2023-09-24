
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, FloatType
import random
from faker import Faker
import numpy as np
import os

hadoop='hadoop'
saksham = 'saksham'

def permission(name):
    os.system(f"sudo chown -R {name}:{name} /home/hadoop/dataset")


    # hadoop fs -setfacl -R -m user:saksham:rwx /data    ----> this command used in hadoop to give the access saksham only on data directory

def hd_directory():
    os.system(f"hadoop fs -rm -r /data/*")

def spark_generate():

    permission(saksham)
    hd_directory()
    
    spark = SparkSession \
    .builder \
    .appName("Ride_Data_Generation") \
    .master("local[1]") \
    .getOrCreate()
   

    fake = Faker()
    np.random.seed(42)

    #  generating customer dataset
    num_customers = 50000
    customer_data = []
    generated_ids = set()


    def generate_unique_cus_id():
        while True:
            random_number = random.randint(1000000, 9999999)
            cus_id = f'cus_id{random_number}'
            if cus_id not in generated_ids:
                generated_ids.add(cus_id)
                return cus_id

    for _ in range(num_customers):
        cus_id = generate_unique_cus_id()
        customer_data.append((
            cus_id,
            fake.name(),
            fake.random_int(min=18, max=80),
            fake.random_element(elements=('Male', 'Female')),
            fake.city()
        ))

    customer_df = spark.createDataFrame(customer_data, ["customer_id", "customer_name", "customer_age", "customer_gender", "customer_location"])
    # customer_df.show()
    customer_df.write.mode("overwrite").csv("/home/hadoop/dataset/customer_data", header=True)

    # genrating driver dataset
    num_drivers = 7000
    driver_data=[]

    driver_g_id=set()

    def generate_unique_driver_id():
        while True:
            random_number = random.randint(1000000, 9999999)
            driver_id = f'driver_id{random_number}'
            if driver_id not in driver_g_id:
                driver_g_id.add(driver_id)
                return driver_id

        
        
    for _ in range(num_drivers):
        driver_id = generate_unique_driver_id()
        driver_data.append((
            driver_id,
            fake.name(),
            fake.random_int(min=18, max=80),
            fake.random_element(elements=('Male', 'Female')),
            fake.random_int(min=1, max=20)
        ))
    driver_df = spark.createDataFrame(driver_data, ["driver_id", "driver_name", "driver_age", "driver_gender", "driver_experience_years"])
    driver_df.write.mode("overwrite").csv("/home/hadoop/dataset/driver_data", header=True)
    # driver_df.show()


    # Loading train data
    train_data = spark.read.csv('/mnt/d/Company Capstone Assignment/train.csv', header=True, inferSchema=True)

    # loading driver_id and customer_id in a python list
    driver_ids = [row[0] for row in driver_data]
    customer_ids=[row[0] for row in customer_data]

        # Define UDF to calculate fare
    def calculate_fare(trip_duration):
        return trip_duration * 0.1

    calculate_fare_udf = udf(calculate_fare, FloatType())

    # udf calling --> picking randomly id's from the lists and filling into rows of driver_id and customer_id respectively, to get realistic data
    def assign_random_driver_id():
        return random.choice(driver_ids)

    def assign_random_customer_id():
        return random.choice(customer_ids)

    udf_assign_random_driver_id = udf(assign_random_driver_id, StringType())
    udf_assign_random_customer_id = udf(assign_random_customer_id, StringType())

    # Add driver_id and customer_id columns to train_data
    ride_data = train_data.withColumn("driver_id", udf_assign_random_driver_id())
    ride_data = ride_data.withColumn("customer_id", udf_assign_random_customer_id())
    ride_data = ride_data.withColumn("fare", calculate_fare_udf(col("trip_duration")))
    # Save ride data
    ride_data.write.mode("overwrite").csv("/home/hadoop/dataset/ride_data", header=True, timestampFormat="yyyy-MM-dd' 'HH:mm:ss")

    #ride_data.show()
    
    # Stop Spark session
    spark.stop()
    # --> changing the permission again
    permission(hadoop)
    




