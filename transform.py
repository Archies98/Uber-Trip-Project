import pyarrow.parquet as pq
import datetime as dt

# read the parquet file using pyarrow
trips = pq.read_table("data/yellow_tripdata_2024-05.parquet")

# convert parquet data to a dataframe
df = trips.to_pandas()

# drop duplicate rows from dataframe if any
df.drop_duplicates().reset_index()

# separate datetime data in a separate dimension table as per datamodel
datetime_dim = df.copy()
datetime_dim = datetime_dim[['tpep_pickup_datetime', 'tpep_dropoff_datetime']]

# set the names of columns to match the data model
datetime_dim.columns = ['pickup_datetime', 'dropoff_datetime']

# set index for primary key of datetime_dim dimension table
datetime_dim['datetime_id'] = datetime_dim.index

# extract the hour, day, month and day of week from pickup datetime
datetime_dim['pickup_hour'] = datetime_dim['pickup_datetime'].dt.hour
datetime_dim['pickup_day'] = datetime_dim['pickup_datetime'].dt.day
datetime_dim['pickup_month'] = datetime_dim['pickup_datetime'].dt.month
# dt.weekdays and dt.dayofweek are aliases of each other
datetime_dim['pickup_weekday'] = datetime_dim['pickup_datetime'].dt.dayofweek

# extract the hour, day, month and day of week from dropoff datetime
datetime_dim['dropoff_hour'] = datetime_dim['dropoff_datetime'].dt.hour
datetime_dim['dropoff_day'] = datetime_dim['dropoff_datetime'].dt.day
datetime_dim['dropoff_month'] = datetime_dim['dropoff_datetime'].dt.month
# dt.weekdays and dt.dayofweek are aliases of each other
datetime_dim['dropoff_weekday'] = datetime_dim['dropoff_datetime'].dt.dayofweek

# separate fare data in a separate dimension table as per data model
fare_amount_dim = df.copy()
fare_amount_dim = fare_amount_dim[['fare_amount', 'extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge','Airport_fee']]

# set index for primary key of fare_amount_dim dimension table
fare_amount_dim['fare_amount_id'] = fare_amount_dim.index

# dictionary to convert ratecode number to ratecode name, from data dictionary
ratecode_dict = {
    1: 'Standard rate',
    2: 'JFK',
    3:'Newark',
    4:'Nassau or Westchester',
    5:'Negotiated fare',
    6:'Group ride'
}

# separate ratecode data in a separate dimension table as per data model
ratecode_dim = df.copy()
ratecode_dim = ratecode_dim[['RatecodeID']]

# rename column to match data model
ratecode_dim.columns = ['ratecode']

# set index for primary key of ratecode_dim dimension table
ratecode_dim['ratecode_id'] = ratecode_dim.index

print(ratecode_dim.ratecode.info())
print(ratecode_dim['ratecode'].max())

# add column that contains ratecode name for corresponding ratecode id

ratecode_name = []

for ratecode in ratecode_dim['ratecode']:
    # if ratecode found in dict, add name otherwise add 'Not Available'
    ratecode_name.append(ratecode_dict.get(ratecode,'Not Available'))

# add the ratecode name array to the dataframe
ratecode_dim['ratecode_name'] = ratecode_name

# create the fact table
fact_table = df.copy()

# set index for fact table
fact_table['trip_id'] = fact_table.index

# join the dimension tables and the fact_table to get all the necessary columns in the fact table
fact_table = fact_table.merge(ratecode_dim, left_on='trip_id', right_on='ratecode_id') \
             .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
             .merge(fare_amount_dim, left_on='trip_id', right_on='fare_amount_id') \
             [['trip_id','VendorID', 'datetime_id', 'passenger_count',
               'trip_distance', 'ratecode_id', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'fare_amount_id']]

# rename fact table columns to match data model
fact_table.columns = ['trip_id','vendor_id', 'datetime_id', 'passenger_count', 'trip_distance', 'ratecode_id', 'store_and_fwd_flag', 'pickup_zone', 'dropoff_zone', 'fare_amount_id']
