import os
from minio import Minio, S3Error
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim
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

        self.spark = SparkSession.builder \
            .master("local[*]") \
            .appName('PySpark') \
            .getOrCreate()

    def data_preview(self, df):
        try:
            df.createOrReplaceTempView("GAMER_DATA")
            for c in df.columns:
                if c != "user_id" and c != "age":
                    query = f"""
                            SELECT {c}, count(*) as count_{c}
                            FROM GAMER_DATA
                            GROUP BY {c};
                            """
                    column_count = self.spark.sql(query)
                    column_count.show()
        except Exception as e:
            print(f"Logging error: {e}")

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
            print(f"Logging error: {e}")

    def check_values_from_df(self, df, numeric_col):
        df.createOrReplaceTempView("GAMER_DATA")
        string_emissions = []
        threshold_list_numeric = []
        total_rows = df.count()
        five_percent_threshold = total_rows * 0.05

        for c in df.columns:
            if c in numeric_col:
                # тут находятся примерные пороги для числовых значений
                query = f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {c}) FROM GAMER_DATA;"
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
                            FROM GAMER_DATA
                            GROUP BY {c}
                            ORDER BY count ASC;
                            """
                    column_count = self.spark.sql(query)
                    first_row = column_count.first()

                    if first_row:
                        column_value, count_rows = first_row[0], first_row[1]
                        # возможные выбросы строковых значений
                        if count_rows < five_percent_threshold:
                            string_emissions.append({
                                "column_name": c,
                                "column_value": column_value,
                                "count": count_rows
                            })

        return threshold_list_numeric, string_emissions

    def clear_datest(self, df, numeric_col ):
        try:
            print(f"Размер датасета до очистки: {df.count()}")
            threshold_list_numeric, string_emissions = self.check_values_from_df(df, numeric_col)

            clear_df = df.dropDuplicates()

            avg_row = clear_df.select([sf.avg(c).alias(c) for c in numeric_col]).first()
            avg_dict = avg_row.asDict() if avg_row else {}
            clear_df = clear_df.fillna(avg_dict)
            for c in clear_df.columns:
                if c not in numeric_col:
                    clear_df = clear_df.withColumn(c, trim(col(c)))

                else:
                    threshold_min, threshold_max = next(
                        ((th.get('threshold_min'), th.get('threshold_max')) for th in threshold_list_numeric
                         if th.get('column_name') == c), (None, None))
                    clear_df = clear_df.filter((col(c) >= threshold_min) & (col(c) <= threshold_max))

            print(f"Размер датасета после очистки: {clear_df.count()}")

            return clear_df
        except Exception as e:
            print(f"Logging error: {e}")

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
            print(f"Logging error: {e}")

    def upload_to_postgresql(self):
        pass

    def upload_to_clickhouse(self):
        pass

    def run_pipeline(self):
        try:
            df = self.download_file_from_minio()
            numeric_col = [c for c, dtype in df.dtypes if dtype in ['int', 'bigint', 'float', 'double']]
            # self.data_preview(df)
            self.check_values_from_df(df, numeric_col)
            self.get_df_info(df)
            self.clear_datest(df, numeric_col)
        except Exception as e:
            print(f"Logging error: {e}")


idk = ProcessingAndLoadingBySpark("gaming_addiction.csv")
idk.run_pipeline()
