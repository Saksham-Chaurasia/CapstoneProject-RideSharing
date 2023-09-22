from pyspark.sql import SparkSession

def spark_hive_store():
    
    spark = SparkSession \
    .builder \
    .appName("Ride_Data_Storing") \
    .master("local[1]") \
    .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()


    spark.sql("create database if not exists rideSharing")

    # spark.sql("show databases").show()

    # creating table and schema

    # --> this is not correct --> just put it for easy understanding customer_schema = """customer_id string, customer_name string, customer_age int,customer_gender string, customer_location string"""

    # spark.sql("create table if not exists rideSharing.customer (customer_id string, customer_name string, customer_age int,customer_gender string, customer_location string)row format delimited fields terminated by ',' tblproperties(\"skip.header.line.count\"=\"1\")")
    spark.sql("create table if not exists rideSharing.customer \
              (customer_id string, customer_name string, customer_age int, \
              customer_gender string, customer_location string)row format delimited fields terminated by ',' \
              tblproperties(\"skip.header.line.count\"=\"1\")")


    # --> easy understanding of schema driver_schema = """driver_id string, driver_name string, driver_age int,driver_gender string, driver_experience_years int"""
 
    spark.sql("create table if not exists rideSharing.driver \
              (driver_id string, driver_name string, driver_age int, \
              driver_gender string, driver_experience_years int) \
              row format delimited fields terminated by ',' tblproperties(\"skip.header.line.count\"=\"1\")")

    # ride_schema="""id string, vendor_id string, pickup_datetime timestamp,dropoff_datetime timestamp, passenger_count int, pickup_longitude float,pickup_latitude float, dropoff_longitude float, dropoff_latitude float,store_and_fwd_flag string, trip_duration int, cus_id string, driver_id string,fare float"""

    spark.sql("create table if not exists rideSharing.ride \
              (id string, vendor_id string, pickup_datetime string, \
              dropoff_datetime string, passenger_count int, pickup_longitude float, \
              pickup_latitude float, dropoff_longitude float, dropoff_latitude float, \
              store_and_fwd_flag string, trip_duration int, driver_id string, customer_id string, fare float) \
              row format delimited fields terminated by ',' tblproperties(\"skip.header.line.count\"=\"1\")")


    # loading data to hive table


    # customer
    spark.sql("LOAD DATA INPATH 'hdfs://localhost:9000/data/customer' INTO TABLE rideSharing.customer")
    
    # driver
    spark.sql("LOAD DATA INPATH 'hdfs://localhost:9000/data/driver' INTO TABLE rideSharing.driver")
    
    # ride
    spark.sql("LOAD DATA INPATH 'hdfs://localhost:9000/data/ride' INTO TABLE rideSharing.ride")

    







