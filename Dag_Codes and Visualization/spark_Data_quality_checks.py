from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType


def quality_checks():

    spark = SparkSession \
        .builder \
        .appName("DataQualityChecks") \
        .getOrCreate()

    check_customer = spark.read.csv("hdfs://localhost:9000/data/customer", sep=',',header=True,inferSchema=True)
    check_driver = spark.read.csv("hdfs://localhost:9000/data/driver", sep=',',header=True,inferSchema=True)
    check_ride = spark.read.csv("hdfs://localhost:9000/data/ride",sep=',',header=True,inferSchema=True)

    # to return to the dag so when check_failed becomes true my spark_hive storing will not going to work

    check_Failed = False
    # Data Quality Checks---> practice 

    # creating a list of all the dataframes
    dataframes_checking = [check_customer, check_driver, check_ride]

    # now taking each dataframe to check missing values in column one by one
    for df in dataframes_checking:
        print(f"Checking missing values in DataFrame: {df}")
        
        # taking all the column names
        columns_to_check = df.columns

        # iterating through all columns
        for column in columns_to_check:
            missing_count = df.filter(col(column).isNull()).count()
            if missing_count > 0:
                check_Failed = True
                print(f"Missing values in '{column}': {missing_count}")

        print("\n")

    # printing to see it is checking or not

    # checking fare should not be negative

    column = "fare"
    min_value = 0.0

    # if invalid_count is there it will count and print for me

    invalid_count = check_ride.filter(col(column) < min_value).count()
    if invalid_count > 0:
        check_Failed = True
        print(f"Invalid values in '{column}' has negative values): {invalid_count}")

    # checking for schema verification ----------------------------------

    expected_customer_schema= StructType([
        StructField("customer_id", IntegerType(), nullable=True),
        StructField("customer_name", StringType(), nullable=True),
        StructField("customer_age", IntegerType(), nullable=True),
        StructField("customer_gender", StringType(), nullable=True),
        StructField("customer_location", StringType(), nullable=True),
    ])

    if check_customer.schema == expected_customer_schema:
        print("Customer schema is correct")
    else:
        print("Customer schema is not correct")
        check_Failed = True
        actual_fields = [field.name for field in check_ride.schema.fields]
        expected_fields = [field.name for field in expected_ride_schema.fields]
        print(f"Actual Fields: {actual_fields}")
        print()
        print(f"Expected Fields: {expected_fields}")
            

    expected_driver_schema = StructType([
        StructField("driver_id", IntegerType(), nullable=True),
        StructField("driver_name", StringType(), nullable=True),
        StructField("driver_age", IntegerType(), nullable=True),
        StructField("driver_gender", StringType(), nullable=True),
        StructField("driver_experience_years", IntegerType(), nullable=True),
    ])


    if check_driver.schema == expected_driver_schema:
        print("Driver schema is correct")

    else:
        print("Driver schema is not correct")
        actual_fields = [field.name for field in check_ride.schema.fields]
        expected_fields = [field.name for field in expected_ride_schema.fields]
        check_Failed = True
        print(f"Actual Fields: {actual_fields}")
        print()
        print(f"Expected Fields: {expected_fields}")
            

    expected_ride_schema = StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("vendor_id", IntegerType(), nullable=True),
        StructField("pickup_datetime", TimestampType(), nullable=True),
        StructField("dropoff_datetime", TimestampType(), nullable=True),
        StructField("passenger_count", IntegerType(), nullable=True),
        StructField("pickup_longitude", DoubleType(), nullable=True),
        StructField("pickup_latitude", DoubleType(), nullable=True),
        StructField("dropoff_longitude", DoubleType(), nullable=True),
        StructField("dropoff_latitude", DoubleType(), nullable=True),
        StructField("store_and_fwd_flag", StringType(), nullable=True),
        StructField("trip_duration", IntegerType(), nullable=True),
        StructField("driver_id", IntegerType(), nullable=True),
        StructField("customer_id", IntegerType(), nullable=True),
        StructField("fare", DoubleType(), nullable=True),
        
    ])


    if check_ride.schema == expected_ride_schema:
        print("Ride schema is correct")
    else:
        print("Ride schema is not correct")
        actual_fields = [field.name for field in check_ride.schema.fields]
        expected_fields = [field.name for field in expected_ride_schema.fields]
        check_Failed = True
        print(f"Actual Fields: {actual_fields}")
        print()
        print(f"Expected Fields: {expected_fields}")

    
    return not check_Failed
    spark.stop()
            
        