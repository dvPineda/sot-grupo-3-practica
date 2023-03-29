import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import split, from_unixtime, unix_timestamp, date_format, concat_ws, lit, to_date, col, year

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

horoscopo_path = "s3://sot-practica-grupo-3/curated/horoscopo/*.parquet"
user_id_profile_path = "s3://sot-practica-grupo-3/curated/user_id_profile/*.parquet"

# Join Profile - Horoscopo
horoscopo = spark.read.format("parquet").load(horoscopo_path)
profiles = spark.read.format("parquet").load(user_id_profile_path)

start_split = split(horoscopo['startDate'], '-')
horoscopo = horoscopo.withColumn("start_month",from_unixtime(unix_timestamp(start_split.getItem(1),'MMM'),'MM'))
horoscopo = horoscopo.withColumn("start_day", start_split.getItem(0))

end_split = split(horoscopo['endDate'], '-')
horoscopo = horoscopo.withColumn("end_month",from_unixtime(unix_timestamp(end_split.getItem(1),'MMM'),'MM'))
horoscopo = horoscopo.withColumn("end_day", end_split.getItem(0))

profiles_horoscopo = profiles.alias("profile").join(horoscopo.alias("h"), col("profile.registered").between(
    date_format(concat_ws("-", year(col("profile.registered")), col("start_month"), col("start_day")),"yyyy-MM-dd").cast("date"), #start date
    date_format(concat_ws("-", year(col("profile.registered")), col("end_month"), col("end_day")),"yyyy-MM-dd").cast("date") #end date
))

profiles_horoscopo_def = profiles_horoscopo.select(col("#id"), col("gender"), col("age"), col("country"), col("registered"), col("h.sign").alias("horoscopo"))

# Join Profile - Horoscopo - Continent
continent_path = "s3://sot-practica-grupo-3/curated/continentCountry/*.parquet"
continent = spark.read.format("parquet").load(continent_path)

profile_horoscopo_continent = profiles_horoscopo_def.alias("pro_h").join(continent.alias("con"), col("pro_h.country") == col("con.country"), "left")
result_df = profile_horoscopo_continent.select("pro_h.#id", "pro_h.gender", "pro_h.age", "pro_h.country", "pro_h.registered", "pro_h.horoscopo", "con.continent", "con.sub_region")
result_df.write.format("parquet").mode("overwrite").save("s3://sot-practica-grupo-3-analytics/data/profiles")

songs_path = "s3://sot-practica-grupo-3/curated/user_songs/*.parquet"
songs = spark.read.format("parquet").load(songs_path)
songs.write.format("parquet").mode("overwrite").save("s3://sot-practica-grupo-3-analytics/data/songs")

horoscopo.write.format("parquet").mode("overwrite").save("s3://sot-practica-grupo-3-analytics/data/horoscopo")
continent.write.format("parquet").mode("overwrite").save("s3://sot-practica-grupo-3-analytics/data/continent")


job.commit()