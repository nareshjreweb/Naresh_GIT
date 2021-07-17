#importing initial required modules
import os
import sys
import datetime
import pandas as pd

#setting up the environment variables
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
#os.environ["HBASE_HOME"] = "/opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/lib/hbase/"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/lib/hbase/*.jar --master local[2] pyspark-shell'

os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")
sys.path.insert(0, "/opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/lib/hbase/*")

#importing anotehr set of required modules for pyspark
from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pyspark.sql as psql

#main method
if __name__ == "__main__":
    #spark instance creation
    spark = SparkSession.builder.appName("CapReadTableData").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    #setting up the required jars for spark to pull data from hbase-hive table
    conf = (SparkConf().set("spark.jars", "file://opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/lib/hive/lib/hive-hbase-handler-1.1.0-cdh5.15.1.jar"))

    conf.set('spark.driver.extraClassPath', '/opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/lib/hive/lib/hive-hbase-handler-1.1.0-cdh5.15.1.jar')
    conf.set('spark.executor.extraClassPath', '/opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/lib/hive/lib/hive-hbase-handler-1.1.0-cdh5.15.1.jar')


    #setting up spark and sql context
    sc = spark.sparkContext
    sqlContext = SQLContext.getOrCreate(sc)

    #Create a Hive Context

    hive_context = HiveContext(sc)
    #patients contact info data from hive and setting in a data frame
    Patients_Contact_Info_df = hive_context.sql("select * from Patients_Contact_Info")
    #Threshold reference data from hive and setting in a data frame
    threshold_reference_table_df = hive_context.sql("select * from threshold_reference_table")
    #Patient vital info data from hive and setting in a data frame
    patients_vital_info_df = hive_context.sql("select * from patients_vital_info")
    #creating another patient vital info 2 dataframe to rename column customer id to patient id to perform a join
    patients_vital_info_df_2 = patients_vital_info_df.withColumnRenamed("CustomerID", "patientid")
    #performing a join between patient vital info2 and patient contact info so that we have a consolidated info in one dataframe
    patient_vital_contacts_df = patients_vital_info_df_2.join(Patients_Contact_Info_df, on="patientid",how="leftouter")

    #blank list instantiated
    L_Patient_Alerts_blanket = []

    #for joined df - patient_vital_contacts_df and threshold_reference_table_df, converting them into a list
    List_Patient_Vital_Contact_Info = patient_vital_contacts_df.collect()
    List_threshold_reference_table = threshold_reference_table_df.collect()

    #iterating the list to perform logic
    for pvi_row in List_Patient_Vital_Contact_Info:
        for j in List_threshold_reference_table:
            if pvi_row['BP']!='' and pvi_row['BP']>=j['low_value'] and j['attribute']=='bp' and pvi_row['BP']<=j['high_value'] and j['alert_flag']==1 and pvi_row['age']>=j['low_age_limit'] and pvi_row['age']<=j['high_age_limit']:
                L_Patient_Alerts=[pvi_row['patientid'],pvi_row['patientname'],pvi_row['age'],pvi_row['patientaddress'],pvi_row['phone_number'],pvi_row['admitted_ward'],pvi_row['BP'],pvi_row['HeartBeat'],pvi_row['Message_time'],j['alert_message'],str(datetime.datetime.now())]
                L_Patient_Alerts_blanket.append(L_Patient_Alerts)
            if pvi_row['HeartBeat']!='' and pvi_row['HeartBeat']>=j['low_value'] and j['attribute']=='heartBeat' and pvi_row['HeartBeat']<=j['high_value'] and j['alert_flag']==1 and pvi_row['age']>=j['low_age_limit'] and pvi_row['age']<=j['high_age_limit']:
                L_Patient_Alerts=[pvi_row['patientid'],pvi_row['patientname'],pvi_row['age'],pvi_row['patientaddress'],pvi_row['phone_number'],pvi_row['admitted_ward'],pvi_row['BP'],pvi_row['HeartBeat'],pvi_row['Message_time'],j['alert_message'],str(datetime.datetime.now())]
                L_Patient_Alerts_blanket.append(L_Patient_Alerts)

    #pulling the required fields in pandas blanket dataframe
    df_Patient_Alerts_blanket = pd.DataFrame(L_Patient_Alerts_blanket,columns =['patientid','patientname','age','patientaddress','phone_number','admitted_ward','bp','heartBeat','input_message_time','alert_message','alert_generated_time'])
    #Patient_Alerts_blanket = spark.readStream.format("kafka").option("kafka.bootstrap.servers","52.87.40.157:9092").option("subscribe","patients_vital_info_topic").option("failOnDataLoss", "false").load()
    #ds = df_Patient_Alerts_blanket.select(to_json(struct([col(c).alias(c) for c in df_Patient_Alerts_blanket.columns]))).writeStream.format("kafka").option("kafka.bootstrap.servers", "52.87.40.157:9092").option("topic", "Alerts_Message").option("failOnDataLoss", "false").start()
    
    #converting the blanket dataframe into spark data frame
    sdf = spark.createDataFrame(df_Patient_Alerts_blanket)
    #pushing data to kafka topic, Alerts_Message
    ds = sdf.selectExpr("CAST(patientid AS STRING) AS key", "to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", "52.87.40.157:9092").option("topic", "Alerts_Message").option("failOnDataLoss", "false").save()
    #ds = sdf.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers", "52.87.40.157:9092").option("topic", "Alerts_Message").option("failOnDataLoss", "false").start()
    #ds.awaitTermination()
