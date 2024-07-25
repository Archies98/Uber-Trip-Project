from sqlalchemy import create_engine
import pandas as pd
import datetime

# it is assumed that a Redshift Serverless Instance exists in your AWS account.

# create a connection to the database in AWS Redshift
conn = create_engine('postgresql://admin:Admin123@workgroup1.278525518778.us-east-1.redshift-serverless.amazonaws.com:5439/dev')

# read the fact_table file
df = pd.read_csv('fact_table.csv', dtype={'trip_id':int,'vendor_id':int, 'datetime_id':int, 'passenger_count':float, 'trip_distance':float, 'ratecode_id':int, 'store_and_fwd_flag':str, 'pickup_zone':int, 'dropoff_zone':int, 'fare_amount_id':int})
# send the fact_table to the database 'dev' in Redshift
df.to_sql('fact_table', conn, index=False, if_exists='replace')

# read the  _dim dimension file
df = pd.read_csv('datetime_dim.csv', dtype={'pickup_datetime': datetime.datetime,'dropoff_datetime': datetime.datetime,'datetime_id':int,'pickup_hour':int,'pickup_day':int,'pickup_month':int,'pickup_weekday':int,'dropoff_hour':int,'dropoff_day':int,'dropoff_month':int,'dropoff_weekday':int})
# send the datetime_dim dimension table to the database 'dev' in Redshift
df.to_sql('datetime_dim', conn, index=False, if_exists='replace')

# read the  _dim dimension file
df = pd.read_csv('ratecode_dim.csv', dtype={'ratecode':int, 'ratecode_id':int, 'ratecode_name':str})
# send the ratecode_dim dimension table to the database 'dev' in Redshift
df.to_sql('ratecode_dim', conn, index=False, if_exists='replace')

# read the  _dim dimension file
df = pd.read_csv('fare_amount_dim.csv', dtype = {'fareamount_id':int,'fare_amount':float, 'extra':float,'mta_tax':float,'tip_amount':float,'tolls_amount':float,'improvement_surcharge':float,'total_amount':float,'congestion_surcharge':float,'Airport_fee':float})
# send the fare_amount_dim dimension table to the database 'dev' in Redshift
df.to_sql('fare_amount_dim', conn, index=False, if_exists='replace')
