# Delta-Lake-Pyspark-Helper-Class

Delta Lake is the optimized storage layer that provides the foundation for storing data and tables in the Databricks Lakehouse Platform. Delta Lake is open source software that extends Parquet data files with a file-based transaction log for ACID transactions and scalable metadata handling.

![image](https://user-images.githubusercontent.com/39345855/203801714-e7050ab0-c597-4718-a709-64ab1fabc7ad.png)



* Documentation :http://delta-lake-helper-class.s3-website-us-east-1.amazonaws.com/ 




This Python Class will have you up and running with Delta lakes in no time. There are methods, particularly for beginners, that make it simple to insert update delete into delta lake.


----------------------------------------------------------------------------------
###  Step 1 Install Jar Files


Upload JAR files on S3 Bucket and then add the path in your glue script as shown in image and copy paste the code and make sure to change base s3 path


`Link https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.0.0/delta-core_2.12-1.0.0.jar`_


![image](https://user-images.githubusercontent.com/39345855/203795334-ede5f648-be37-4a39-a07d-76b6d791a2a1.png)

----------------------------------------------------------------------------------

###  Step 2 Copy Python Template

Copy paste the Python Template in your Glue code and start using it Refer to examples shown below

Go TO Section Python Class and Copy Entire Class



----------------------------------------------------------------------------------
###  Step 3 Examples

```
from pyspark import SparkConf, SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext

# Creating Instance
# This will be the path where your delta lake already exists or where you want to build one.
helper = DeltaLakeHelper(delta_lake_path="s3a://glue-learn-begineers/deltalake/delta_table")

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = helper.spark
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ====================================================
"""Create Spark Data Frame """
# ====================================================
data = impleDataUpd = [
  (1, "this is inser 1 ", "Sales", "RJ", 81000, 30, 23000, 827307999),
  (2, "this is inser 2", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
  (3, "this is inser 3", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
]
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
df_write = spark.createDataFrame(data=data, schema=columns)
helper.insert_overwrite_records_delta_lake(spark_df=df_write)

# ====================================================
"""READ FROM DELTA LAKE  """
# ====================================================
df_read = helper.read_delta_lake()
print("READ", df_read.show())

# ====================================================
"""UPDATE DELTA LAKE"""
# ====================================================
helper.update_records_delta_lake(condition="emp_id = '3'",
                               value_to_set={"employee_name": "'THIS WAS UPDATE ON DELTA LAKE'"})

# ====================================================
""" DELETE DELTA LAKE"""
# ====================================================
helper.delete_records_delta_lake(condition="emp_id = '4'")

# ====================================================
""" FIND ONE AND UPDATE OR UPSERT DELTA LAKE """
# ====================================================
new_data = [
  (2, "this is update on delta lake ", "Sales", "RJ", 81000, 30, 23000, 827307999),
  (11, "This should be append ", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
]

columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
usr_up_df = spark.createDataFrame(data=new_data, schema=columns)

helper.upsert_records_delta_lake(old_data_key='emp_id',
                               new_data_key='emp_id',
                               new_spark_df=usr_up_df)

# ====================================================
""" Compaction DELTA Prune Older Version and Create larger Files """
# ====================================================
helper.compact_table(num_of_files=2)
helper.delete_older_files_versions()

# ====================================================
""" Create Manifest File for Athena """
# ====================================================
helper.generate_manifest_files()

job.commit()

```


----------------------------------------------------------------------------------


####  Become a  Contributor  ?

Please feel free to fork the project, make your changes, and submit a merge request.

Github : https://github.com/soumilshah1995/Delta-Lake-Pyspark-Helper-Class


----------------------------------------------------------------------------------

#### Videos Tutorials

* How to Write | Read | Query Delta lake using AWS Glue and Athena for Queries for Beginners https://www.youtube.com/watch?v=4HUgZksc1eE


* Getting started with Delta lakes Pyspark and AWS Glue (Glue Connector) https://www.youtube.com/watch?v=xpU6JPWZ9Pw


* INSERT | UPDATE |DELETE| READ | CRUD |on Delta lake(S3) using Glue PySpark Custom Jar Files & Athena  https://www.youtube.com/watch?v=M0Q9AwnuW-w


* How do I use Glue to convert existing small parquet files to larger parquet files on Delta Lake https://www.youtube.com/watch?v=AGWcGlraqEg&t=455s


* Upsert | Find One and Update in Delta Lake Using Glue Pyspark and Convert Small File into Large File https://youtu.be/861mVVgmXw4

----------------------------------------------------------------------------------

####  Authors

Soumil Nitin Shah

I earned a Bachelor of Science in Electronic Engineering and a double master’s in Electrical and Computer Engineering. I have extensive expertise in developing scalable and high-performance software applications in Python. I have a YouTube channel where I teach people about Data Science, Machine learning, Elastic search, and AWS. I work as data collection and processing Team Lead at Jobtarget where I spent most of my time developing Ingestion Framework and creating microservices and scalable architecture on AWS. I have worked with a massive amount of data which includes creating data lakes (1.2T) optimizing data lakes query by creating a partition and using the right file format and compression. I have also developed and worked on a streaming application for ingesting real-time streams data via kinesis and firehose to elastic search


Website : http://soumilshah.com/


Github : https://github.com/

----------------------------------------------------------------------------------




