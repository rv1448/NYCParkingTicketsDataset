from pyspark.sql.types import StructField, StructType, StringType, LongType,IntegerType,LongType
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("spark://172.31.2.140:7077") \
            .appName("NYC Data load") \
            .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
            .config("spark.hadoop.fs.s3a.fast.upload","true") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.speculation", "false") \
            .appName("NYC Data load").enableHiveSupport().getOrCreate()
spark.sql("set spark.sql.shuffle.partitions=500")




from pyspark.sql.functions import expr
from pyspark.sql.functions import unix_timestamp, col
from pyspark.sql.types import TimestampType
from pyspark.sql.types import StringType
from pyspark.sql import functions as sf
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import year, month
## Load Data from S3 
df = spark.read.csv("s3://nycparking2015/Parking_Violations_Issued_-_Fiscal_Year_2015.csv",inferSchema=True,header=True) 
 
## Remove Extra Spaces from the columns 
k = df.columns
m = []
for x in k:
  m.append(x.replace(' ','_'))

## Rename columns with special characters 
df = df.toDF(*m) \
  .withColumnRenamed("Days_Parking_In_Effect____","Days_Parking_In_Effect") \
  .withColumnRenamed("Unregistered_Vehicle?","Unregistered_Vehicle")

## load up fines data from S3 that will give the amount fined for violation
df_fines = spark.read.csv("s3://nycparking2015/Violation_CD_fines.csv",inferSchema=True,header=True)

df = df.withColumn("TimeStamp", sf.concat(df.Issue_Date,sf.lit(' '),df.Violation_Time.substr(1, 4))) \
  .withColumn("TimeStamp_new",to_timestamp("TimeStamp", "MM/dd/yyyy HHmm").cast("timestamp")) \
  .withColumn("hour",df.Violation_Time.substr(1, 2)).withColumn("year",year("TimeStamp_new")).withColumn("month",month("TimeStamp_new")) 

#df = df.withColumn("FINE",df.join(df_fines, df["violation_code"] == df_fines["CODE "],"left_outer")["FINE_AMOUNT"])
##.withColumn("Fine_amount",df.join(df_fines, df["violation_code"] == df_fines["CODE "],"left_outer").select(df.*, col("FINE_AMOUNT"))
df = df.join(df_fines, df["violation_code"] == df_fines["CODE "],"left_outer")
 
## Drop duplicates and records with Timestamp is null 
df = df.dropDuplicates().filter("TimeStamp_new is not null").repartition("year","month")
df.cache().createOrReplaceTempView("Inputdata")


## DIM violation data preparation loading 
dimviolation = spark.sql("select  ROW_NUMBER() OVER(ORDER BY 1) as violation_sk,Violation_Code,Violation_Precinct,Violation_Location,Violation_description,Violation_Post_Code from ( \
select Violation_Code,Violation_Precinct,Violation_Location,Violation_description,Violation_Post_Code, \
ROW_NUMBER() OVER(PARTITION BY Violation_Code,Violation_precinct order by length(Violation_description)) as ack \
from inputdata) a \
where ack = 1").repartition("Violation_Code").cache()


spark.sql("""DROP TABLE IF EXISTS dim_violation_tbl""")
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS dim_violation_tbl(violation_sk int,Violation_Code int, Violation_Precinct int,Violation_Location int,Violation_description string,Violation_Post_Code string)
STORED AS PARQUET LOCATION 's3a://emrnycdataloading/dim_violation_tbl'""")


dimviolation.write.mode("overwrite").insertInto("dim_violation_tbl")



## DIM vehicle data preparation loading and table creation
spark.sql("""DROP TABLE IF EXISTS dim_vehicle_tbl""")
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS dim_vehicle_tbl(vehicle_sk int,Plate_ID string,Plate_Type string, Vehicle_Body_Type string,Vehicle_Make string,
Vehicle_Expiration_Date	string, Vehicle_Color string ,Registration_State string,Unregistered_Vehicle int,Vehicle_Year int )
STORED AS PARQUET LOCATION 's3a://emrnycdataloading/dim_vehicle_tbl'""")



dimvehicle = spark.sql("select ROW_NUMBER() OVER(ORDER BY 1) as vehicle_sk, \
          b.Plate_ID,b.Plate_Type,b.Vehicle_Body_Type,b.Vehicle_Make,b.Vehicle_Expiration_Date,b.Vehicle_Color, \
          b.Registration_State, \
          b.Unregistered_Vehicle,b.Vehicle_Year \
          from \
          (select Plate_ID, Registration_State, Plate_Type,Vehicle_Body_Type,Vehicle_Make, Vehicle_Expiration_Date,Vehicle_Color,Unregistered_Vehicle, Vehicle_Year, \
          ROW_NUMBER() OVER(PARTITION BY Plate_id,Plate_Type order by Issue_Date desc) as rnk from Inputdata) b  \
where rnk = 1").repartition("Plate_id").cache().coalesce(10)

dimvehicle.write.mode("overwrite").insertInto("dim_vehicle_tbl")



## DIM TIME data preparation loading and table creation
dimtime = spark.sql("""select ROW_NUMBER() OVER( ORDER BY 1) as time_sk , event_time,weekday,weekofyear,hour,
day,quarter,month,year
from ( select TimeStamp_new as event_time, day(TimeStamp_new) as day,  weekofyear(TimeStamp_new) as weekofyear,quarter(TimeStamp_new) as quarter,  
hour(TimeStamp_new) as hour,date_format(TimeStamp_new, 'EEEE') as weekday, 
year(TimeStamp_new) as year, month(Timestamp_new) as month,
ROW_NUMBER() OVER(PARTITION BY TimeStamp_new order by issue_date desc) as rnk  
from inputdata 
) b 
where rnk = 1 """).repartition(year("event_time")).cache()


spark.sql("""DROP TABLE IF EXISTS dim_time_tbl""")
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS dim_time_tbl(time_sk	int	, event_time timestamp,weekday string ,weekofyear int,hour int,day int,quarter int,month int)
STORED AS ORC LOCATION 's3a://emrnycdataloading/dim_time_tbl' PARTITIONED BY (year int)""")


dimtime.write.mode("overwrite").insertInto("dim_time_tbl")




## DIM issuer table creation and data preparation, loading  
dimissuer = spark.sql("""select ROW_NUMBER() OVER(ORDER BY 1) as issuer_sk, Issuer_Code, Issuing_Agency, Issuer_Command, Issuer_Squad,Issuer_Precinct 
from ( select ROW_NUMBER() OVER(PARTITION BY Issuer_Code,Issuer_Precinct order by Issue_date desc) as ack ,
Issuer_Precinct, Issuer_Code ,Issuing_Agency,Issuer_Command, Issuer_Squad from inputdata ) a  
where ack = 1""").repartition("Issuer_Code","Issuer_Precinct").coalesce(20).cache()


spark.sql("""DROP TABLE IF EXISTS dim_issuer_tbl""")
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS dim_issuer_tbl(issuer_sk int,Issuer_Code int,Issuing_Agency string,Issuer_Command string,Issuer_Squad string,Issuer_Precinct int)
STORED AS PARQUET LOCATION 's3a://emrnycdataloading/dim_issuer_tbl'""")

dimissuer.write.mode("overwrite").insertInto("dim_issuer_tbl")


## FACT DATA PREP and LOADING 
FCT_TICKS = spark.sql("""select issuer_sk,time_sk,vehicle_sk,FINE_AMOUNT, Street_Code1, Street_Code2, Street_Code3, 
Summons_Number,a.Plate_ID,Time_First_Observed,House_Number, Street_Name, Intersecting_Street,
Date_First_Observed, Law_Section, Sub_Division, From_Hours_In_Effect, To_Hours_In_Effect,violation_sk,a.year,a.month
from inputdata a join dim_vehicle_tbl b  
on a.plate_id = b.plate_id and a.Plate_type = b.plate_type  
join dim_time_tbl c on a.TimeStamp_new  = c.event_time  
join dim_violation_tbl d on a.Violation_Code = d.Violation_Code and a.Violation_Precinct = d.Violation_Precinct   
join dim_issuer_tbl e on a.Issuer_Code = e.Issuer_Code  and a.Issuer_Precinct = e.Issuer_Precinct""").repartition("year","month").cache()


spark.sql("""DROP TABLE IF EXISTS FACT_TICKETS""")
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS FACT_TICKETS(issuer_sk int,time_sk int,vehicle_sk int,FINE_AMOUNT double,Street_Code1 int,Street_Code2 int, Street_Code3 int, Summons_Number bigint, Plate_ID string, Time_First_Observed	string, House_Number string,
Street_Name	string,Intersecting_Street	string, Date_First_Observed	string, Law_Section	int,Sub_Division string,From_Hours_In_Effect string, To_Hours_In_Effect string,violation_sk int) 
STORED AS PARQUET LOCATION 's3a://emrnycdataloading/FACT_TICKETS_TBL' PARTITIONED BY (year int,month int)""")
FCT_TICKS.write.mode("overwrite").insertInto("FACT_TICKETS")

