from pyspark.sql.types import StructField, StructType, StringType, LongType,IntegerType,LongType
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import unix_timestamp, col
from pyspark.sql.types import TimestampType
from pyspark.sql.types import StringType
from pyspark.sql import functions as sf
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import year, month


spark = SparkSession.builder.master("spark://172.31.12.12:7077") \
            .appName("NYC Data load") \
            .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
            .config("spark.hadoop.fs.s3a.fast.upload","true") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.speculation", "false") \
            .appName("NYC Data load").enableHiveSupport().getOrCreate()
spark.sql("set spark.sql.shuffle.partitions=500")


df_batch =  spark.read.csv("s3://nycparking2015/Parking_Violations_Issued_-_Fiscal_Year_2015.csv",inferSchema=True,header=True) 


##Remove extra spaces in the colums 
FileColumns = df_batch.columns
DataFrameColumns = []

for i in FileColumns:
  DataFrameColumns.append(i.replace(' ','_'))

## Rename columns with special characters 
df_batch = df_batch.toDF(*m) \
  .withColumnRenamed("Days_Parking_In_Effect____","Days_Parking_In_Effect") \
  .withColumnRenamed("Unregistered_Vehicle?","Unregistered_Vehicle")

## load up fines data from S3 that will give the amount fined for violation
df_batch_fines = spark.read.csv("s3://nycparking2015/Violation_CD_fines.csv",inferSchema=True,header=True)

df_batch = df_batch.withColumn("TimeStamp", sf.concat(df_batch.Issue_Date,sf.lit(' '),df_batch.Violation_Time.substr(1, 4))) \
  .withColumn("TimeStamp_new",to_timestamp("TimeStamp", "MM/dd/yyyy HHmm").cast("timestamp")) \
  .withColumn("hour",df_batch.Violation_Time.substr(1, 2)).withColumn("year",year("TimeStamp_new")).withColumn("month",month("TimeStamp_new")) 

#df = df.withColumn("FINE",df.join(df_fines, df["violation_code"] == df_fines["CODE "],"left_outer")["FINE_AMOUNT"])
##.withColumn("Fine_amount",df.join(df_fines, df["violation_code"] == df_fines["CODE "],"left_outer").select(df.*, col("FINE_AMOUNT"))
df_batch = df_batch.join(df_batch_fines, df_batch["violation_code"] == df_batch_fines["CODE "],"left_outer")
 
## Drop duplicates and records with Timestamp is null 
df_batch = df_batch.dropDuplicates().filter("TimeStamp_new is not null").repartition("year","month")
df_batch.cache().createOrReplaceTempView("Inputdata_batch")



## find the latest record to update back to dim tables 
dimviolation_batch = spark.sql("select Violation_Code,Violation_Precinct,Violation_Location,Violation_description,Violation_Post_Code from ( \
select Violation_Code,Violation_Precinct,Violation_Location,Violation_description,Violation_Post_Code, \
ROW_NUMBER() OVER(PARTITION BY Violation_Code,Violation_precinct order by length(Violation_description)) as ack \
from Inputdata_batch) a \
where ack = 1").repartition("Violation_Code").cache()

dimvehicle_batch = spark.sql("select b.Plate_ID,b.Plate_Type,b.Vehicle_Body_Type,b.Vehicle_Make,b.Vehicle_Expiration_Date,b.Vehicle_Color, \
          b.Registration_State, \
          b.Unregistered_Vehicle,b.Vehicle_Year \
          from \
          (select Plate_ID, Registration_State, Plate_Type,Vehicle_Body_Type,Vehicle_Make, Vehicle_Expiration_Date,Vehicle_Color,Unregistered_Vehicle, Vehicle_Year, \
          ROW_NUMBER() OVER(PARTITION BY Plate_id,Plate_Type order by TimeStamp_new desc) as rnk from Inputdata_batch) b  \
where rnk = 1").repartition("Plate_id").cache().coalesce(10)

dimtime_batch = spark.sql("""select ROW_NUMBER() OVER( ORDER BY 1) as time_sk , event_time,weekday,weekofyear,hour,
day,quarter,month,year 
from ( select TimeStamp_new as event_time, day(TimeStamp_new) as day,  weekofyear(TimeStamp_new) as weekofyear,quarter(TimeStamp_new) as quarter,  
hour(TimeStamp_new) as hour,date_format(TimeStamp_new, 'EEEE') as weekday, 
year(TimeStamp_new) as year, month(Timestamp_new) as month,
ROW_NUMBER() OVER(order by TimeStamp_new desc) as rnk  
from inputdata_batch 
) b 
where rnk = 1 """).repartition(year("event_time")).cache()

dimissuer_batch = spark.sql("""select  Issuer_Code, Issuing_Agency, Issuer_Command, Issuer_Squad,Issuer_Precinct 
from ( select ROW_NUMBER() OVER(PARTITION BY Issuer_Code,Issuer_Precinct order by TimeStamp_new desc) as ack ,
Issuer_Precinct, Issuer_Code ,Issuing_Agency,Issuer_Command, Issuer_Squad from Inputdata_batch ) a  
where ack = 1""").repartition("Issuer_Code","Issuer_Precinct").coalesce(20).cache()

dimissuer_batch.createOrReplaceTempView("dimissuer_batch_view")
dimvehicle_batch.createOrReplaceTempView("dimvehicle_batch_view")
dimviolation_batch.createOrReplaceTempView("dimviolation_batch_view")
dimtime_batch.createOrReplaceTempView("dimtime_batch_view")


## Prepare dataset with latest data 
df_vehicle_final = spark.sql("""
select ROW_NUMBER() OVER(ORDER BY 1) as vehicle_sk, subquery.Plate_ID,subquery.Plate_Type,
        subquery.Vehicle_Body_Type,subquery.Vehicle_Make,subquery.Vehicle_Expiration_Date,subquery.Vehicle_Color, 
        subquery.Registration_State,subquery.Unregistered_Vehicle,subquery.Vehicle_Year
from (
select  dest.Plate_ID,dest.Plate_Type,
        src.Vehicle_Body_Type,src.Vehicle_Make,src.Vehicle_Expiration_Date,src.Vehicle_Color, 
        src.Registration_State,src.Unregistered_Vehicle,src.Vehicle_Year 
          from 
dimvehicle_batch_view src join dim_vehicle_tbl dest 
on src.Plate_id = dest.plate_id 
and  src.Plate_Type = dest.Plate_Type 

union 

select  src.Plate_ID,src.Plate_Type,src.Vehicle_Body_Type,src.Vehicle_Make,src.Vehicle_Expiration_Date,src.Vehicle_Color, 
        src.Registration_State, 
        src.Unregistered_Vehicle,src.Vehicle_Year  from 
dimvehicle_batch_view src left join dim_vehicle_tbl dest 
on src.Plate_id = dest.plate_id 
and  src.Plate_Type = dest.Plate_Type 
Where  dest.plate_type is null 
) subquery  
""").repartition("Plate_id").cache().coalesce(10)

df_violation_final = spark.sql("""select ROW_NUMBER() OVER(ORDER BY 1) as violation_sk, 
subquery.Violation_Code,subquery.Violation_Precinct,
subquery.Violation_Location,subquery.Violation_description,subquery.Violation_Post_Code 
from 
(
select dest.Violation_Code,dest.Violation_Precinct,
src.Violation_Location,src.Violation_description,src.Violation_Post_Code
from dimviolation_batch_view src join dim_violation_tbl dest 
on src.violation_Code = dest.violation_Code
and src.violation_precinct = dest.violation_precinct

union 


select src.Violation_Code,src.Violation_Precinct,
src.Violation_Location,src.Violation_description,src.Violation_Post_Code
from dimviolation_batch_view src left join dim_violation_tbl dest 
on src.violation_Code = dest.violation_Code
and src.violation_precinct = dest.violation_precinct
where dest.violation_precinct is null
) subquery
""").repartition("Violation_Code").cache()


df_issuer_final = spark.sql("""select ROW_NUMBER() OVER(ORDER BY 1) as issuer_sk,
 subquery.Issuer_Code, subquery.Issuing_Agency, subquery.Issuer_Command, subquery.Issuer_Squad,subquery.Issuer_Precinct 
from 
(
select dest.Issuer_Code, dest.Issuing_Agency, src.Issuer_Command, src.Issuer_Squad,src.Issuer_Precinct 
 from dimissuer_batch_view  src join dim_issuer_tbl dest
 on src.Issuer_Code = dest.Issuer_Code
 and src.Issuer_Precinct = dest.Issuer_Precinct
 
 union 
 
 select src.Issuer_Code, src.Issuing_Agency, src.Issuer_Command, src.Issuer_Squad,src.Issuer_Precinct 
 from  dimissuer_batch_view src left join dim_issuer_tbl dest
 on src.Issuer_Code = dest.Issuer_Code
 and src.Issuer_Precinct = dest.Issuer_Precinct
 where dest.Issuer_Precinct is null and dest.Issuer_Code is null 
) subquery 
""").repartition("Issuer_Code","Issuer_Precinct").coalesce(20).cache()


df_time_final = spark.sql("""
select ROW_NUMBER() OVER(ORDER BY 1) as time_sk,  event_time,weekday,weekofyear,hour,
day,quarter,month,year 
from 
(
select src.event_time,src.weekday,src.weekofyear,src.hour,
src.day,src.quarter,src.month,src.year 
from dimtime_batch_view src 
WHERE NOT EXISTS (select 1 from dim_time_tbl dest 
where src.event_time = dest.event_time)
) subquery
""").repartition(year("event_time")).cache() 

 
## write the dataset overwrite/append 
df_vehicle_final.write.mode("overwrite").insertInto("dim_vehicle_tbl")
df_time_final.write.mode("append").insertInto("dim_time_tbl")
df_issuer_final.write.mode("overwrite").insertInto("dim_issuer_tbl")
df_violation_final.write.mode("overwrite").insertInto("dim_violation_tbl")



FCT_TICKS_FINAL = spark.sql("""select issuer_sk,time_sk,vehicle_sk,FINE_AMOUNT, Street_Code1, Street_Code2, Street_Code3, 
Summons_Number,a.Plate_ID,Time_First_Observed,House_Number, Street_Name, Intersecting_Street,
Date_First_Observed, Law_Section, Sub_Division, From_Hours_In_Effect, To_Hours_In_Effect,violation_sk,a.year,a.month
from Inputdata_batch a join dim_vehicle_tbl b  
on a.plate_id = b.plate_id and a.Plate_type = b.plate_type  
join dim_time_tbl c on a.TimeStamp_new  = c.event_time  
join dim_violation_tbl d on a.Violation_Code = d.Violation_Code and a.Violation_Precinct = d.Violation_Precinct   
join dim_issuer_tbl e on a.Issuer_Code = e.Issuer_Code  and a.Issuer_Precinct = e.Issuer_Precinct
""").repartition("year","month").cache()

## WRITE TO THE FACT TABLE 
FCT_TICKS_FINAL.write.mode("append").insertInto("FACT_TICKETS")