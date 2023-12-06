from pyspark.sql.functions import *
from pyspark.sql import DataFrame

def sample_transform(input_df: DataFrame) -> DataFrame:
    inter_df = input_df.where(input_df['that_column'] == \
                              lit('hobbit')).groupBy('another_column').agg(sum('yet_another').alias('new_column'))
    output_df = inter_df.select('another_column', 'new_column', \
                                when(col('new_column') > 10, 'yes').otherwise('no').alias('indicator')).where(
                col('indicator') == lit('yes'))
    return output_df



# mineF.

#targetDF=spark.read.format('delta').table('silver.products_silver')

def prepare_products_for_scd2(targetDF, df):
	join_columns=["product_code"]
	targetDF=targetDF
	sourceDF=df

	# Step 1 - left join to new data the main table
	joinDF=(
	    sourceDF.join(
	        targetDF.where(col("active_status")=='Y'),
	        *join_columns,
	        'leftouter'
	    )
	    .select(
	        sourceDF['*'],
	        targetDF["ProductId"].alias("target_ProductId"),
	        targetDF["product_code"].alias("target_product_code"), targetDF["product_name"].alias("target_product_name"), targetDF["category"].alias("target_category"), targetDF["description"].alias("target_description"), targetDF["gender"].alias("target_gender"), targetDF["color"].alias("target_color"), targetDF["size"].alias("target_size")
	    )
	)

	# Step 2 - Filter out only changed records
	filterDF=(joinDF.filter(
	        xxhash64(col("product_name"), col("category"), col("description"), col("gender"), col("color"), col("size"))
	        != xxhash64(col("target_product_name"), col("target_category"), col("target_description"), col("target_gender"), col("target_color"), col("target_size"))
	    ))

	# Step 3 - create merge key
	mergeDF=filterDF.withColumn('MERGEKEY', concat(*join_columns))

	# Step 4 - create null merge key for matching records
	dummyDF=filterDF.filter('target_ProductId is not null').withColumn('MERGEKEY', lit(None))

	# Step 5 - combine step 3 and 4
	scdDF=mergeDF.union(dummyDF)
	scdDF.createOrReplaceTempView('scdDF')

	# Step 6 - apply merge statement

	# skipped


	return scdDF
