# Delta-Lake-Pyspark-Helper-Class

Delta Lake is the optimized storage layer that provides the foundation for storing data and tables in the Databricks Lakehouse Platform. Delta Lake is open source software that extends Parquet data files with a file-based transaction log for ACID transactions and scalable metadata handling.

This Python Class will have you up and running with Delta lakes in no time. There are methods, particularly for beginners, that make it simple to insert update delete, read and append into delta lake

![image](https://user-images.githubusercontent.com/39345855/203801714-e7050ab0-c597-4718-a709-64ab1fabc7ad.png)

* Documentation :http://delta-lake-helper-class.s3-website-us-east-1.amazonaws.com/ 


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
### Examples

##### Define your imports 
```
from pyspark import SparkConf, SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = helper.spark
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

```
##### Create instance and Provide your Delta Lake Path
```
helper = DeltaLakeHelper(delta_lake_path="s3a://glue-learn-begineers/deltalake/delta_table")
```

##### Adding Some Dummy data into Delta lake 
```
data = impleDataUpd = [
  (1, "this is inser 1 ", "Sales", "RJ", 81000, 30, 23000, 827307999),
  (2, "this is inser 2", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
  (3, "this is inser 3", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
  (4, "this is inser 3", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
]
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
df_write = spark.createDataFrame(data=data, schema=columns)
helper.insert_overwrite_records_delta_lake(spark_df=df_write)```
```

##### Appending into Delta Lakes ?
```
data = impleDataUpd = [
(5, "this is append", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
]
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
df_append = spark.createDataFrame(data=data, schema=columns)
helper.append_records_delta_lake(spark_df=df_write)```
```

##### Updating Item in delta lake 
```
helper.update_records_delta_lake(condition="emp_id = '3'",
    value_to_set={"employee_name": "'THIS WAS UPDATE ON DELTA LAKE'"})

```

##### Deleting Items On Delta Lake based on Condition
```
helper.delete_records_delta_lake(condition="emp_id = '4'")
```

##### Convert smaller Parquert files into larger parquet files and delete older version 
```
helper.compact_table(num_of_files=2)
helper.delete_older_files_versions()
```

##### Generate Manifest File for Athena ?
```
helper.generate_manifest_files()
```

#### Entire Code 
```

##### Appending into Delta Lakes ?
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


try:
    import os
    import sys

    import pyspark
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, asc, desc
    from pyspark.sql.functions import *
    from delta.tables import *
    from delta.tables import DeltaTable

    print("All modules are loaded .....")

except Exception as e:
    print("Some modules are missing {} ".format(e))


class DeltaLakeHelper(object):
    """
    Delta Lakes Python helper class that aids in basic operations such as inserting, updating, deleting, merging, removing older files and versions, and generating Athena manifest files.
    """

    def __init__(self, delta_lake_path: str):

        self.spark = self.__create_spark_session()
        self.delta_lake_path = delta_lake_path
        self.delta_df = None

    def generate_manifest_files(self):
        """

        Generates Manifest file for Athena

        :return: Bool
        """
        self.delta_df.generate("symlink_format_manifest")
        return True

    def __generate_delta_df(self):
        try:
            if self.delta_df is None:
                self.delta_df = DeltaTable.forPath(self.spark, self.delta_lake_path)
        except Exception as e:
            pass

    def compact_table(self, num_of_files=10):
        """

        Converts smaller parquert files into larger Files

        :param num_of_files: Int
        :return: Bool

        """
        df_read = self.spark.read.format("delta") \
            .load(self.delta_lake_path) \
            .repartition(num_of_files) \
            .write.option("dataChange", "false") \
            .format("delta") \
            .mode("overwrite") \
            .save(self.delta_lake_path)
        return True

    def delete_older_files_versions(self):
        """

        Deletes Older Version and calls vacuum(0)

        :return: Bool
        """
        self.__generate_delta_df()
        self.delta_df.vacuum(0)
        return True

    def insert_overwrite_records_delta_lake(self, spark_df, max_record_per_file='10000'):
        """
        Inserts into Delta Lake

        :param spark_df: Pyspark Dataframe
        :param max_record_per_file: str ie max_record_per_file= "10000"
        :return:Bool
        """
        spark_df.write.format("delta") \
            .mode("overwrite") \
            .option("maxRecordsPerFile", max_record_per_file) \
            .save(self.delta_lake_path)

        return True

    def append_records_delta_lake(self, spark_df, max_record_per_file="10000"):
        """

        Append data into Delta lakes

        :param spark_df: Pyspark Dataframe
        :param max_record_per_file: str ie max_record_per_file= "10000"
        :return: Bool
        """
        spark_df.write.format("delta") \
            .mode('append') \
            .option("maxRecordsPerFile", max_record_per_file) \
            .save(self.delta_lake_path)
        return True

    def update_records_delta_lake(self, condition="", value_to_set={}):
        """

        Set the value on delta lake

        :param condition : Str Example:  condition="emp_id = '3'"
        :param value_to_set: Dict IE  value_to_set={"employee_name": "'THIS WAS UPDATE ON DELTA LAKE'"}
        :return: Bool
        """
        self.__generate_delta_df()
        self.delta_df.update(condition, value_to_set)
        return True

    def upsert_records_delta_lake(self, old_data_key, new_data_key, new_spark_df):
        """

        Find one and update into delta lake
        If records is found it will update if not it will insert into delta lakes
        See Examples on How to use this

        :param old_data_key: Key is nothing but Column on which you want to merge or upsert data into delta lake
        :param new_data_key: Key is nothing but Column on which you want to merge or upsert data into delta lake
        :param new_spark_df: Spark DataFrame
        :return: Bool
        """
        self.__generate_delta_df()
        dfUpdates = new_spark_df

        self.delta_df.alias('oldData') \
            .merge(dfUpdates.alias('newData'), f'oldData.{old_data_key} = newData.{new_data_key}') \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        return True

    def delete_records_delta_lake(self, condition=""):
        """

        Set the value on delta lake

        :param condition:Str IE condition="emp_id = '4'"
        :return:Bool
        """
        self.__generate_delta_df()
        self.delta_df.delete(condition)
        return True

    def read_delta_lake(self):
        """

        Reads from Delta lakes

        :return: Spark DF
        """
        df_read = self.spark.read.format("delta").load(self.delta_lake_path)
        return df_read

    def __create_spark_session(self):
        self.spark = SparkSession \
            .builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        return self.spark


# ====================================================
"""Create Spark Data Frame """
# ====================================================
data = impleDataUpd = [
  (1, "this is inser 1 ", "Sales", "RJ", 81000, 30, 23000, 827307999),
  (2, "this is inser 2", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
  (3, "this is inser 3", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
  (4, "this is inser 3", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
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




