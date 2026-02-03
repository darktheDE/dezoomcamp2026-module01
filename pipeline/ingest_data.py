#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
import click


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    """
    Ingest CSV data into a PostgreSQL database.
    """
    print(f"Connecting to database {pg_db} at {pg_host}:{pg_port}...")
    # Create the engine
    # Using psycopg (standard for SQLAlchemy 2.0+)
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Schema definition for yellow taxi data
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = prefix + 'yellow_tripdata_2021-01.csv.gz'

    print(f"Reading data from {url}...")
    # Use iterator and chunksize for memory efficiency
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000
    )

    # Process the first chunk to create the table
    try:
        df = next(df_iter)
    except StopIteration:
        print("The file is empty.")
        return

    # Create table schema (no data)
    print(f"Creating/Replacing table {target_table}...")
    df.head(0).to_sql(name=target_table, con=engine, if_exists='replace')

    # Insert the first chunk
    df.to_sql(name=target_table, con=engine, if_exists='append')
    print(f"Inserted first chunk ({len(df)} rows)")

    # Insert remaining chunks
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            
            df.to_sql(name=target_table, con=engine, if_exists='append')
            
            t_end = time()
            print(f"Inserted another chunk ({len(df)} rows), took {t_end - t_start:.3f} seconds")
            
        except StopIteration:
            print("Data ingestion complete.")
            break


if __name__ == '__main__':
    run()