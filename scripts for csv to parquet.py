import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql import functions as f 
from awsglue.dynamicframe import DynamicFrame 
import logging
import os


logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

# Creating a handler for CloudWatch
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info('My Log Message')

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1734185237365 = glueContext.create_dynamic_frame.from_catalog(database="csv_database", table_name="product", transformation_ctx="AmazonS3_node1734185237365")

logger.info('Print schema of AmazonS3_node1734185237365')
AmazonS3_node1734185237365.printSchema()

count = AmazonS3_node1734185237365.count()
print('Number of rows in AmazonS3_node1734185237365 dynamic_frame ',count)
logger.info('count for frame is {}'.format(count))


logger.info("Source schema:")
AmazonS3_node1734185237365.printSchema()

apply_mapping_node = ApplyMapping.apply(
    frame=AmazonS3_node1734185237365,
    mappings=[
        ('rank', 'bigint', 'Rank', 'bigint'),
        ('name', 'string', 'Name', 'string'),
        ('platform', 'string', 'Platform', 'string'),
        ('year', 'bigint', 'Year', 'bigint'),
        ('genre', 'string', 'Genre', 'string'),
        ('publisher', 'string', 'Publisher', 'string'),
        ('na_sales', 'double', 'NA_Sales', 'double'),
        ('eu_sales', 'double', 'EU_Sales', 'double'),
        ('jp_sales', 'double', 'JP_sales', 'double'),
        ('other_sales', 'double', 'Other_Sales', 'double'),
        ('global_sales', 'double', 'Global_Sales', 'double')
    ],
    transformation_ctx='apply_mapping_node'
)

logger.info("Mapped schema:")
apply_mapping_node.printSchema()

spark_DF = apply_mapping_node.toDF()
spark_DF.show()

spark_DF = (
    spark_DF.withColumn(
        'Ratings',
        f.when(spark_DF["Name"] == "Wii Sports", 4.8)
        .when(spark_DF["Name"] == "Super Mario Bros.", 4.4)
        .when(spark_DF["Name"] == "Mario Kart Wii", 3.8)
        .when(spark_DF["Name"] == "Wii Sports Resort", 4.9)
        .when(spark_DF["Name"] == "Pokemon Red/Pokemon Blue", 5)
        .when(spark_DF["Name"] == "Tetris", 4)
        .when(spark_DF["Name"] == "New Super Mario Bros.", 4.6)
        .when(spark_DF["Name"] == "Wii Play", 3)
        .when(spark_DF["Name"] == "New Super Mario Bros. Wii", 4)
        .when(spark_DF["Name"] == "Duck Hunt", 3.5)
        .when(spark_DF["Name"] == "Nintendogs", 4.9)
        .when(spark_DF["Name"] == "Mario Kart DS", 3.2)
        .when(spark_DF["Name"] == "Pokemon Gold/Pokemon Silver", 2)
        .when(spark_DF["Name"] == "Wii Fit", 4.1)
        .when(spark_DF["Name"] == "Wii Fit Plus", 3.1)
        .when(spark_DF["Name"] == "Kinect Adventures!", 2.2)
        .when(spark_DF["Name"] == "Grand Theft Auto V", 5)
        .when(spark_DF["Name"] == "Grand Theft Auto: San Andreas", 4.7)
        .when(spark_DF["Name"] == "Super Mario World", 4.1)
        .when(spark_DF["Name"] == "Brain Age: Train Your Brain in Minutes a Day", 3.6)
        .otherwise(0.0)  # Default rating if game_name is not in the list
    )
)

spark_DF.show()

logger.info("Converting spark dataframe to glue DataFrame")
output_dynamic_dataframe = DynamicFrame.fromDF(spark_DF, glueContext, 'output_dynamic_dataframe') 



# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(
            frame=AmazonS3_node1734185237365, 
            ruleset=DEFAULT_DATA_QUALITY_RULESET, 
            publishing_options={
                "dataQualityEvaluationContext": "EvaluateDataQuality_node1734184391826", "enableDataQualityResultsPublishing": True
            }, 
            additional_options={
                "dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"
            }
    )

AmazonS3_node1734185368563 = glueContext.write_dynamic_frame.from_options(
            frame=output_dynamic_dataframe, connection_type="s3", 
            format="glueparquet", 
            connection_options={
                "path": "s3://video-games/source_output/parquet_product/", "partitionKeys": []
            }, 
            format_options={"compression": "snappy"}, 
            transformation_ctx="AmazonS3_node1734185368563"
    )

job.commit()