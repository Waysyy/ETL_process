import datetime
import os
from minio import Minio, S3Error
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim, current_date
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as sf


class ProcessingAndLoadingBySpark:
    def __init__(self, file_name):
        self.file_name = file_name
        load_dotenv()
        self.access_key = os.getenv('ACCESS_KEY')
        self.secret_key = os.getenv('SECRET_KEY')
        self.bucket_name = os.getenv('BUCKET_NAME')
        self.endpoint = os.getenv('ENDPOINT')

        hadoop_dir = os.path.abspath("./hadoop")
        os.environ["HADOOP_HOME"] = hadoop_dir
        os.environ["PATH"] += os.path.pathsep + os.path.join(hadoop_dir, "bin")

        current_dir = os.path.dirname(os.path.abspath(__file__))
        local_jar_path = os.path.join(current_dir, "jars", "clickhouse-jdbc-all-0.9.8.jar")
        clean_jar_path = local_jar_path.replace('\\', '/')
        jar_path = f"file:///{clean_jar_path}"

        self.spark = SparkSession.builder \
            .master("local[*]") \
            .appName('PySpark') \
            .config("spark.jars", jar_path) \
            .getOrCreate()

    def data_preview(self, df):
        try:
            df.createOrReplaceTempView("TEMP_TABLE")
            for c in df.columns:
                if c != "user_id" and c != "age":
                    query = f"""
                            SELECT {c}, count(*) as count_{c}
                            FROM TEMP_TABLE
                            GROUP BY {c};
                            """
                    column_count = self.spark.sql(query)
                    column_count.show()
        except Exception as e:
            print(f"ERROR: {e}")

    def download_file_from_minio(self):
        try:
            client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=False
            )
            try:
                # Для демонстрации
                client.stat_object(self.bucket_name, self.file_name)
            except S3Error:
                client.fput_object(self.bucket_name, self.file_name, f"./raw_csv/{self.file_name}")

            client.fget_object(
                bucket_name=self.bucket_name,
                object_name=self.file_name,
                file_path=f"./downloads/{self.file_name}"
            )

            df = self.spark.read.csv(f"./downloads/{self.file_name}", header=True, inferSchema=True)

            return df
        except Exception as e:
            print(f"ERROR: {e}")

    def check_values_from_df(self, df, numeric_col):
        df.createOrReplaceTempView("TEMP_TABLE")
        string_emissions = []
        threshold_list_numeric = []
        total_rows = df.count()
        one_percent_threshold = total_rows * 0.01

        for c in df.columns:
            if c in numeric_col:
                # тут находятся примерные пороги для числовых значений
                query = f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {c}) FROM TEMP_TABLE;"
                median_res = self.spark.sql(query).first()
                median = median_res[0] if median_res and median_res[0] is not None else 0

                threshold_list_numeric.append({
                    "column_name": c,
                    "threshold_max": median * 3,
                    "threshold_min": median / 3
                })

            else:
                if "id" not in c.lower():
                    query = f"""
                            SELECT {c}, count(*) as count
                            FROM TEMP_TABLE
                            GROUP BY {c}
                            ORDER BY count ASC;
                            """
                    column_count = self.spark.sql(query)
                    first_row = column_count.first()

                    if first_row:
                        column_value, count_rows = first_row[0], first_row[1]
                        # возможные выбросы строковых значений
                        if count_rows < one_percent_threshold:
                            string_emissions.append({
                                "column_name": c,
                                "column_value": column_value,
                                "count": count_rows
                            })

        return threshold_list_numeric, string_emissions

    def clear_datest(self, df, numeric_col):
        try:
            print(f"Размер датасета до очистки: {df.count()}")
            threshold_list_numeric, string_emissions = self.check_values_from_df(df, numeric_col)
            # string_emissions лучше анализировать и подгонять под конкретный датасет, пока не использую
            date_col = [c for c, dtype in df.dtypes if dtype in ['date', 'timestamp']]
            clear_df = df.dropDuplicates()

            for c in clear_df.columns:
                if c in numeric_col:
                    threshold_min, threshold_max = next(
                        ((th.get('threshold_min'), th.get('threshold_max')) for th in threshold_list_numeric
                         if th.get('column_name') == c), (None, None))
                    if threshold_min and threshold_max:
                        clear_df = clear_df.withColumn(c, when(((col(c) <= threshold_min) | (col(c) >= threshold_max)),
                                                               None).otherwise(col(c)))
                elif c in date_col:
                    clear_df = clear_df.withColumn(c, when((col(c) > current_date()), None).otherwise(col(c)))
                else:
                    clear_df = clear_df.withColumn(c, trim(col(c)))


            avg_row = clear_df.select([sf.avg(c).alias(c) for c in numeric_col]).first()
            avg_dict = avg_row.asDict() if avg_row else {}
            clear_df = clear_df.fillna(avg_dict)

            print(f"Размер датасета после очистки: {clear_df.count()}")

            return clear_df
        except Exception as e:
            print(f"ERROR: {e}")

    def get_df_info(self, df):
        try:
            raw_df = df
            raw_df.show(3)
            raw_df.select(
                [count(when((col(c).isNull() | (col(c).cast("string") == "")), c)).alias(c) for c in
                 raw_df.columns]).show()
            for name, dtype in raw_df.dtypes:
                print(f"Колонка: {name} Тип: {dtype}")
        except Exception as e:
            print(f"ERROR: {e}")

    def upload_to_clickhouse(self, df):
        try:
            url = os.getenv('JDBC_URL')
            user = os.getenv('CLICKHOUSE_USER')
            password = ""
            driver = "com.clickhouse.jdbc.ClickHouseDriver"
            table_name = self.file_name.lower().replace(".csv", "").replace("-", "_")

            # overwrite если надо перезаписать, append если батчами в одну таблицу дозаписать
            df.write \
                .format("jdbc") \
                .option("driver", driver) \
                .option("url", url) \
                .option("user", user) \
                .option("password", password) \
                .option("dbtable", table_name) \
                .mode("overwrite") \
                .save()

            print("Файл успешно загружен в ClickHouse")
        except Exception as e:
            print(f"ERROR: {e}")

    def run_pipeline(self):
        try:
            df = self.download_file_from_minio()
            numeric_col = [c for c, dtype in df.dtypes if dtype in ['int', 'bigint', 'float', 'double']]
            # self.data_preview(df)
            # self.get_df_info(df)
            clear_df = self.clear_datest(df, numeric_col)
            self.upload_to_clickhouse(clear_df)
        except Exception as e:
            print(f"ERROR: {e}")


processing_and_loading_by_spark = ProcessingAndLoadingBySpark("gaming_addiction.csv")
processing_and_loading_by_spark.run_pipeline()
