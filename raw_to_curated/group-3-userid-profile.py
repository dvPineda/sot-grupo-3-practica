import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date
from pyspark.sql.functions import regexp_replace, initcap
from pyspark.sql.functions import col, isnan, when
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node user_songs
user_songs_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": False,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://sot-practica-grupo-3/raw/users-data/userid-timestamp-artid-artname-traid-traname.tsv"
        ],
        "recurse": True,
    },
    transformation_ctx="user_songs_node1",
)

# Script generated for node user_id_profile
user_id_profile_node1679559869141 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "multiline": False,
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://sot-practica-grupo-3/raw/users-data/userid-profile.csv"]
    },
    transformation_ctx="user_id_profile_node1679559869141",
)

user_id_profile_node1679559869141 = user_id_profile_node1679559869141.withColumn("country", when(col("country").isNull(), "not_country").otherwise(col("country")))
user_id_profile_node1679559869141 = user_id_profile_node1679559869141.withColumn("registered",
                                                                           to_date(user_id_profile_node1679559869141["registered"],'M/d/yyyy'))

# Script generated for node Horoscopo
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://sot-practica-grupo-3/raw/horoscope.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node continen-country
continencountry_node1679559621106 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://sot-practica-grupo-3/raw/countryContinent.csv"],
        "recurse": True,
    },
    transformation_ctx="continencountry_node1679559621106",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=user_songs_node1,
    mappings=[
        ("col0", "string", "userid", "string"),
        ("col1", "string", "timestamp", "timestamp"),
        ("col2", "string", "artid", "string"),
        ("col3", "string", "artname", "string"),
        ("col4", "string", "traid", "string"),
        ("col5", "string", "traname", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)
# Script generated for node Drop Fields
DropFields_node1679559711995 = DropFields.apply(
    frame=continencountry_node1679559621106,
    paths=[
        "code_2",
        "country-code",
        "iso_3166_2",
        "region_code",
        "sub_region_code"
    ],
    transformation_ctx="DropFields_node1679559711995",
)



user_id_profile_node1679559869141.toDF().write.format("parquet").mode("overwrite").save("s3://sot-practica-grupo-3/curated/user_id_profile")
S3bucket_node1.toDF().write.format("parquet").mode("overwrite").save("s3://sot-practica-grupo-3/curated/horoscopo")
ApplyMapping_node2.toDF().write.format("parquet").mode("overwrite").save("s3://sot-practica-grupo-3/curated/user_songs")
DropFields_node1679559711995.toDF().write.format("parquet").mode("overwrite").save("s3://sot-practica-grupo-3/curated/continentCountry")


job.commit()
