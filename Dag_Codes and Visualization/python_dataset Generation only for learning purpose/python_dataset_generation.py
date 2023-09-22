from faker import Faker
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import random

fake = Faker()
np.random.seed(42)

#  customer_data generation
# I've taken 50,000  customers, per customer will be 28 rides approx
customer_data = []

for _ in range(50000):
    random_number = random.randint(1000000, 9999999)
    customer_data.append({
        'customer_id': f'cus_id{random_number}',
        'customer_name': fake.name(),
        'customer_age': fake.random_int(min=18, max=80),
        'customer_gender': fake.random_element(elements=('Male', 'Female')),
        'customer_location': fake.city(),
    })

customer_df = pd.DataFrame(customer_data)
save_path = '/home/hadoop/dataset/customer_data.csv'
customer_df.to_csv(save_path, index=False)

# driver_data generation 
# To handle 1 Lakh customers i've chose  7 thousand driver so per driver approx 200 rides will be there
driver_data = []

for _ in range(7000):
    random_number = random.randint(1000000, 9999999)
    driver_data.append({
        'driver_id': f'driver_id{random_number}',
        'driver_name': fake.name(),
        'driver_age': fake.random_int(min=25, max=70),
        'driver_gender': fake.random_element(elements=('Male', 'Female')),
        'driver_experience_years': fake.random_int(min=1, max=20),
    })

driver_df = pd.DataFrame(driver_data)
save_path = '/home/hadoop/dataset/driver_data.csv'
driver_df.to_csv(save_path, index=False)



# Load customer and driver data
customer_data = pd.read_csv('/home/hadoop/dataset/customer_data.csv')
driver_data = pd.read_csv('/home/hadoop/dataset/driver_data.csv')

# Load original train data
train_data = pd.read_csv('/home/hadoop/dataset/train.csv')

# Normalize customer and driver weights
customer_weights = np.random.random(len(customer_data))
customer_weights /= np.sum(customer_weights)

driver_weights = np.random.random(len(driver_data))
driver_weights /= np.sum(driver_weights)

ride_data = []

batch_size = 10000
total_batches = len(train_data) // batch_size + 1

for batch_num, batch_start in enumerate(range(0, len(train_data), batch_size)):
    batch_end = min(batch_start + batch_size, len(train_data))
    batch_train_data = train_data.iloc[batch_start:batch_end]
    
    for _, row in batch_train_data.iterrows():
        cus_id = np.random.choice(customer_data['customer_id'], p=customer_weights)
        driver_id = np.random.choice(driver_data['driver_id'], p=driver_weights)
        
        
        trip_duration = row['trip_duration']
        fare = trip_duration * 0.1

        ride_data.append({
            'ride_id': row['id'],
            'cus_id': cus_id,
            'driver_id': driver_id,
            'vendor_id': row['vendor_id'],
            'pickup_datetime': row['pickup_datetime'],
            'dropoff_datetime': row['dropoff_datetime'],
            'passenger_count': row['passenger_count'],
            'pickup_longitude': row['pickup_longitude'],
            'pickup_latitude': row['pickup_latitude'],
            'dropoff_longitude': row['dropoff_longitude'],
            'dropoff_latitude': row['dropoff_latitude'],
            'store_and_fwd_flag': row['store_and_fwd_flag'],
            'trip_duration': row['trip_duration'],
            'fare': fare,
        })
    
    print(f"Processed batch {batch_num + 1} out of {total_batches}")


ride_df = pd.DataFrame(ride_data)


save_path = '/home/hadoop/dataset/ride_data.csv'
ride_df.to_csv(save_path, index=False)


