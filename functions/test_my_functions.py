import pytest
from my_functions import *
from pandas.testing import assert_frame_equal
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import datetime
    

@pytest.mark.usefixtures("spark_session")
def test_sample_transform(spark_session):


    test_df = spark_session.createDataFrame(
        [
            ('hobbit', 'Samwise', 5),
            ('hobbit', 'Billbo', 50),
            ('hobbit', 'Billbo', 20),
            ('wizard', 'Gandalf', 1000)
        ],
        ['that_column', 'another_column', 'yet_another']
    )
    new_df = sample_transform(test_df)
    assert new_df.count() == 1
    assert new_df.toPandas().to_dict('list')['new_column'][0] == 70


# mine
@pytest.mark.usefixtures("spark_session")
def test_prepare_products_for_scd2(spark_session):

    spark=spark_session

    # ASSEMBLE

    # new data, 1 row new, 1 updated and 1 already in table with no changes
    sourceDF_data=[
        Row(product_code='PR0001', product_name='Product 1', category='changed', description='lalalalal', gender='lala', color='black', size='55', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy___Copy.csv', file_size=240, file_modification_time=datetime.datetime(2023, 12, 6, 20, 23, 31)), 
        Row(product_code='PR0006', product_name='Product 6', category='accessories', description='accessories young girls', gender='female', color='blue', size='UN', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy___Copy.csv', file_size=240, file_modification_time=datetime.datetime(2023, 12, 6, 20, 23, 31)), 
        Row(product_code='PR0098', product_name='Product 100', category='new', description='long skinny', gender='female', color='black', size='32', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy___Copy.csv', file_size=240, file_modification_time=datetime.datetime(2023, 12, 6, 20, 23, 31))
    ]
    sourceDF_schema=StructType([StructField('product_code', StringType(), True), StructField('product_name', StringType(), True), StructField('category', StringType(), True), StructField('description', StringType(), True), StructField('gender', StringType(), True), StructField('color', StringType(), True), StructField('size', StringType(), True), StructField('file_path', StringType(), False), StructField('file_size', LongType(), False), StructField('file_modification_time', TimestampType(), False)])
    df=spark.createDataFrame(sourceDF_data, schema=sourceDF_schema)

    # table with data
    targetDF_data=[
    Row(ProductId=1, product_code='PR0001', product_name='Product 1', category='shoes', description='running shoes', gender='unisex', color='black', size='8', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products.csv', file_size=398, file_modification_time=datetime.datetime(2023, 12, 4, 16, 4, 21), active_status='N', valid_from=datetime.date(2023, 1, 1), valid_to=datetime.date(2023, 12, 4)), 
    Row(ProductId=2, product_code='PR0002', product_name='Product 2', category='pants', description='long pants', gender='female', color='white', size='42', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products.csv', file_size=398, file_modification_time=datetime.datetime(2023, 12, 4, 16, 4, 21), active_status='N', valid_from=datetime.date(2023, 1, 1), valid_to=datetime.date(2023, 12, 4)), 
    Row(ProductId=7, product_code='PR0098', product_name='Product 98', category='pants', description='long skinny', gender='female', color='black', size='32', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy.csv', file_size=350, file_modification_time=datetime.datetime(2023, 12, 5, 18, 55, 59), active_status='Y', valid_from=datetime.date(2023, 12, 5), valid_to=datetime.date(9999, 12, 31)), 
    Row(ProductId=9, product_code='PR0099', product_name='Product 99', category='accessories', description='accessories young girls', gender='female', color='blue', size='UN', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy.csv', file_size=350, file_modification_time=datetime.datetime(2023, 12, 5, 18, 55, 59), active_status='Y', valid_from=datetime.date(2023, 12, 5), valid_to=datetime.date(9999, 12, 31)), 
    Row(ProductId=8, product_code='PR0001', product_name='Product 1', category='changed', description='running shoes', gender='unisex', color='black', size='8', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy.csv', file_size=350, file_modification_time=datetime.datetime(2023, 12, 5, 18, 55, 59), active_status='Y', valid_from=datetime.date(2023, 12, 5), valid_to=datetime.date(9999, 12, 31)), 
    Row(ProductId=10, product_code='PR0002', product_name='Product 2', category='changed', description='long pants', gender='female', color='white', size='42', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy.csv', file_size=350, file_modification_time=datetime.datetime(2023, 12, 5, 18, 55, 59), active_status='Y', valid_from=datetime.date(2023, 12, 5), valid_to=datetime.date(9999, 12, 31)), 
    Row(ProductId=3, product_code='PR0003', product_name='Product 3', category='underwear', description='underwear male', gender='male', color='red', size='M', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products.csv', file_size=398, file_modification_time=datetime.datetime(2023, 12, 4, 16, 4, 21), active_status='Y', valid_from=datetime.date(2023, 1, 1), valid_to=datetime.date(9999, 12, 31)), 
    Row(ProductId=4, product_code='PR0004', product_name='Product 4', category='shoes', description='sport shoes', gender='female', color='blue', size='7', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products.csv', file_size=398, file_modification_time=datetime.datetime(2023, 12, 4, 16, 4, 21), active_status='Y', valid_from=datetime.date(2023, 1, 1), valid_to=datetime.date(9999, 12, 31)), 
    Row(ProductId=5, product_code='PR0005', product_name='Product 5', category='pants', description='long skinny', gender='female', color='black', size='32', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products.csv', file_size=398, file_modification_time=datetime.datetime(2023, 12, 4, 16, 4, 21), active_status='Y', valid_from=datetime.date(2023, 1, 1), valid_to=datetime.date(9999, 12, 31)), 
    Row(ProductId=6, product_code='PR0006', product_name='Product 6', category='accessories', description='accessories young girls', gender='female', color='blue', size='UN', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products.csv', file_size=398, file_modification_time=datetime.datetime(2023, 12, 4, 16, 4, 21), active_status='Y', valid_from=datetime.date(2023, 1, 1), valid_to=datetime.date(9999, 12, 31))
               ]
    targetDF_schema=StructType([StructField('ProductId', LongType(), True), StructField('product_code', StringType(), False), StructField('product_name', StringType(), False), StructField('category', StringType(), True), StructField('description', StringType(), True), StructField('gender', StringType(), True), StructField('color', StringType(), True), StructField('size', StringType(), True), StructField('file_path', StringType(), False), StructField('file_size', LongType(), False), StructField('file_modification_time', TimestampType(), False), StructField('active_status', StringType(), False), StructField('valid_from', DateType(), False), StructField('valid_to', DateType(), False)])
    targetDF_new=spark.createDataFrame(targetDF_data, schema=targetDF_schema)

    
    # expected result
    scdDF_data=[
    Row(product_code='PR0001', product_name='Product 1', category='changed', description='lalalalal', gender='lala', color='black', size='55', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy___Copy.csv', file_size=240, file_modification_time=datetime.datetime(2023, 12, 6, 20, 23, 31), target_ProductId=8, target_product_code='PR0001', target_product_name='Product 1', target_category='changed', target_description='running shoes', target_gender='unisex', target_color='black', target_size='8', MERGEKEY='PR0001'), 
    Row(product_code='PR0098', product_name='Product 100', category='new', description='long skinny', gender='female', color='black', size='32', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy___Copy.csv', file_size=240, file_modification_time=datetime.datetime(2023, 12, 6, 20, 23, 31), target_ProductId=7, target_product_code='PR0098', target_product_name='Product 98', target_category='pants', target_description='long skinny', target_gender='female', target_color='black', target_size='32', MERGEKEY='PR0098'), 
    Row(product_code='PR0098', product_name='Product 100', category='new', description='long skinny', gender='female', color='black', size='32', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy___Copy.csv', file_size=240, file_modification_time=datetime.datetime(2023, 12, 6, 20, 23, 31), target_ProductId=7, target_product_code='PR0098', target_product_name='Product 98', target_category='pants', target_description='long skinny', target_gender='female', target_color='black', target_size='32', MERGEKEY=None), 
    Row(product_code='PR0001', product_name='Product 1', category='changed', description='lalalalal', gender='lala', color='black', size='55', file_path='dbfs:/FileStore/mnt/test_bucket/landing/products/2022_06_09_091705_products___Copy___Copy.csv', file_size=240, file_modification_time=datetime.datetime(2023, 12, 6, 20, 23, 31), target_ProductId=8, target_product_code='PR0001', target_product_name='Product 1', target_category='changed', target_description='running shoes', target_gender='unisex', target_color='black', target_size='8', MERGEKEY=None)
    ]
    scdDF_schema=StructType([StructField('product_code', StringType(), True), StructField('product_name', StringType(), True), StructField('category', StringType(), True), StructField('description', StringType(), True), StructField('gender', StringType(), True), StructField('color', StringType(), True), StructField('size', StringType(), True), StructField('file_path', StringType(), False), StructField('file_size', LongType(), False), StructField('file_modification_time', TimestampType(), False), StructField('target_ProductId', LongType(), True), StructField('target_product_code', StringType(), True), StructField('target_product_name', StringType(), True), StructField('target_category', StringType(), True), StructField('target_description', StringType(), True), StructField('target_gender', StringType(), True), StructField('target_color', StringType(), True), StructField('target_size', StringType(), True), StructField('MERGEKEY', StringType(), True)])
    scdDF_new=spark.createDataFrame(scdDF_data, schema=scdDF_schema)
    scdDF_new=(
        scdDF_new
        .withColumn("file_modification_time", date_format("file_modification_time", "yyyy-MM-dd HH:mm:ss"))
        .toPandas()
        .sort_values(['product_code', 'MERGEKEY']).reset_index(drop=True)
        )
    

    # ACT
    scdDF=prepare_products_for_scd2(targetDF, df)
    scdDF=(
        scdDF
        .withColumn("file_modification_time", date_format("file_modification_time", "yyyy-MM-dd HH:mm:ss"))
        .toPandas()
        .sort_values(['product_code', 'MERGEKEY']).reset_index(drop=True)
        )


    # ASSERT
    assert_frame_equal(scdDF, scdDF_new)

