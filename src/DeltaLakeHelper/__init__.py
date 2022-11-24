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
