#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       02_load_raw.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
#------------------------------------------------------------------------------

import time
from snowflake.snowpark import Session
session = Session.builder.config("connection_name", "pp28980.ap-southeast-3.aws").getOrCreate()
#import snowflake.snowpark.types as T
#import snowflake.snowpark.functions as F

### --> ini adalah untuk import session si snowflake snowpark nya ###

POS_TABLES = ['country', 'franchise', 'location', 'menu', 'truck', 'order_header', 'order_detail']
CUSTOMER_TABLES = ['customer_loyalty']
TABLE_DICT = {
    "pos": {"schema": "RAW_POS", "tables": POS_TABLES},
    "customer": {"schema": "RAW_CUSTOMER", "tables": CUSTOMER_TABLES}
}

### --> ini untuk mendefine table dictionary seperti ketika kita mendefine staging table di SQL Snowflake ###

# SNOWFLAKE ADVANTAGE: Schema detection
# SNOWFLAKE ADVANTAGE: Data ingestion with COPY
# SNOWFLAKE ADVANTAGE: Snowflake Tables (not file-based)

def load_raw_table(session, tname=None, s3dir=None, year=None, schema=None):
    session.use_schema(schema)  ## -> Ini sama ajah seperti kita "USE SCHEMA"
    if year is None:
        location = "@external.frostbyte_raw_stage/{}/{}".format(s3dir, tname)
    else:
        print('\tLoading year {}'.format(year)) 
        location = "@external.frostbyte_raw_stage/{}/{}/year={}".format(s3dir, tname, year)

### --> Ini kita mendefine load raw tablenya (stages), jika ada tidak ada data tahun maka kita akses lokasi data dr folder utama
### --> jika ada data tahunnya, maka kita masuk ke subfolder yang ada berisikan informasi data tahunnya tersebut.

    # we can infer schema using the parquet read option
    df = session.read.option("compression", "snappy") \
                            .parquet(location)  ### -> read file yang berbentuk parquet (VARIANT) yang dicompress dengan snappy
    df.copy_into_table("{}".format(tname)) ### -> memindahkan langsung data VARIANT yg disimpan sementara di df ke staging table
    comment_text = '''{"origin":"sf_sit-is","name":"snowpark_101_de","version":{"major":1, "minor":0},"attributes":{"is_quickstart":1, "source":"sql"}}'''
    sql_command = f"""COMMENT ON TABLE {tname} IS '{comment_text}';"""
    session.sql(sql_command).collect() ### -> menjalankan perintah pemindahan ini 

# SNOWFLAKE ADVANTAGE: Warehouse elasticity (dynamic scaling)

def load_all_raw_tables(session):
    _ = session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()
 ### -> membuat warehouse HOL_WH size warehousenya menjadi X_LARGE ###
    for s3dir, data in TABLE_DICT.items():
        tnames = data['tables']
        schema = data['schema']
        for tname in tnames:
            print("Loading {}".format(tname))
            # Only load the first 3 years of data for the order tables at this point
            # We will load the 2022 data later in the lab
            if tname in ['order_header', 'order_detail']:
                for year in ['2019', '2020', '2021']:
                    load_raw_table(session, tname=tname, s3dir=s3dir, year=year, schema=schema)
            else:
                load_raw_table(session, tname=tname, s3dir=s3dir, schema=schema)

    _ = session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL").collect()
    ### -> mengubah size warehouse menjadi XSMALL lagi untuk menghemat resource ###

def validate_raw_tables(session):
    # check column names from the inferred schema
    for tname in POS_TABLES:
        print('{}: \n\t{}\n'.format(tname, session.table('RAW_POS.{}'.format(tname)).columns))

    for tname in CUSTOMER_TABLES:
        print('{}: \n\t{}\n'.format(tname, session.table('RAW_CUSTOMER.{}'.format(tname)).columns))
### -> validasi apakah data dan schema telah masuk ke table yang tepat ### 

# For local debugging
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        load_all_raw_tables(session)
        validate_raw_tables(session)

### -> kita load isi load_all_raw_tables untuk melihat keseluruhan data  
### -> Kita bisa gunakan juga untuk mengecek apakah data telah masuk ke table yang benar