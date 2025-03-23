from pyspark.sql import SparkSession
import os
import sys
import traceback

class SupplierArticleProcessorSQL:
    def __init__(self, spark):
        self.spark = spark
        self.source_table = os.getenv('SOURCE_TABLE', 'your_catalog.your_database.source_table')
        self.destination_table = os.getenv('DEST_TABLE', 'your_catalog.your_database.destination_table')

    def load_views(self):
        self.spark.sql(f"SELECT * FROM {self.source_table}").createOrReplaceTempView("source_view")
        self.spark.sql(f"SELECT * FROM {self.destination_table}").createOrReplaceTempView("dest_view")

    def scenario_multi_supplier_end(self):
        sql = """
        WITH supplier_count AS (
            SELECT article_sk, COUNT(DISTINCT supplier_nbr) AS supplier_count
            FROM source_view
            GROUP BY article_sk
        )
        SELECT d.*, date_sub(current_date(), 1) AS end_date
        FROM dest_view d
        JOIN supplier_count sc ON d.article_sk = sc.article_sk
        WHERE sc.supplier_count > 1 AND d.end_date IS NULL
        """
        return self.spark.sql(sql)

    def scenario_single_supplier_insert(self):
        sql = """
        WITH supplier_count AS (
            SELECT article_sk, COUNT(DISTINCT supplier_nbr) AS supplier_count
            FROM source_view
            GROUP BY article_sk
        )
        SELECT s.*, current_date() AS start_date, NULL AS end_date
        FROM source_view s
        JOIN supplier_count sc ON s.article_sk = sc.article_sk
        WHERE sc.supplier_count = 1
        """
        return self.spark.sql(sql)

    def scenario_supplier_change(self):
        end_sql = """
        SELECT d.*, date_sub(current_date(), 1) AS end_date
        FROM dest_view d
        JOIN source_view s
          ON d.article_sk = s.article_sk AND d.user_principal_name = s.user_principal_name
        WHERE d.supplier_nbr != s.supplier_nbr AND d.end_date IS NULL
        """
        new_sql = """
        SELECT s.*, current_date() AS start_date, NULL AS end_date
        FROM dest_view d
        JOIN source_view s
          ON d.article_sk = s.article_sk AND d.user_principal_name = s.user_principal_name
        WHERE d.supplier_nbr != s.supplier_nbr AND d.end_date IS NULL
        """
        return self.spark.sql(end_sql), self.spark.sql(new_sql)

    def scenario_new_user(self):
        sql = """
        SELECT src.*, current_date() AS start_date, NULL AS end_date
        FROM (SELECT DISTINCT article_sk, supplier_nbr, user_principal_name FROM source_view) src
        LEFT ANTI JOIN (SELECT DISTINCT article_sk, supplier_nbr, user_principal_name FROM dest_view) dst
        ON src.article_sk = dst.article_sk AND src.supplier_nbr = dst.supplier_nbr AND src.user_principal_name = dst.user_principal_name
        """
        return self.spark.sql(sql)

    def scenario_user_leave(self):
        sql = """
        SELECT d.*, date_sub(current_date(), 1) AS end_date
        FROM (SELECT DISTINCT article_sk, supplier_nbr, user_principal_name FROM dest_view WHERE end_date IS NULL) d
        LEFT ANTI JOIN (SELECT DISTINCT article_sk, supplier_nbr, user_principal_name FROM source_view) s
        ON d.article_sk = s.article_sk AND d.supplier_nbr = s.supplier_nbr AND d.user_principal_name = s.user_principal_name
        """
        return self.spark.sql(sql)

    def scenario_article_stop(self):
        sql = """
        SELECT d.*, date_sub(current_date(), 1) AS end_date
        FROM dest_view d
        LEFT ANTI JOIN (SELECT DISTINCT article_sk FROM source_view) s
        ON d.article_sk = s.article_sk
        WHERE d.end_date IS NULL
        """
        return self.spark.sql(sql)

    def process_articles(self):
        try:
            self.load_views()

            scenario_1_4_df = self.scenario_multi_supplier_end()
            scenario_2_5_df = self.scenario_single_supplier_insert()
            scenario_3_end_df, scenario_3_new_df = self.scenario_supplier_change()
            scenario_6_df = self.scenario_new_user()
            scenario_7_df = self.scenario_user_leave()
            scenario_8_df = self.scenario_article_stop()

            print(f"Scenario 1 & 4 - Multi-supplier end-date count: {scenario_1_4_df.count()}")
            print(f"Scenario 2 & 5 - Single supplier insert count: {scenario_2_5_df.count()}")
            print(f"Scenario 3 - Supplier change end-date count: {scenario_3_end_df.count()}")
            print(f"Scenario 3 - Supplier change new insert count: {scenario_3_new_df.count()}")
            print(f"Scenario 6 - New user insert count: {scenario_6_df.count()}")
            print(f"Scenario 7 - User leave end-date count: {scenario_7_df.count()}")
            print(f"Scenario 8 - Article stop end-date count: {scenario_8_df.count()}")

            final_df = scenario_1_4_df.unionByName(scenario_2_5_df, allowMissingColumns=True) \
                                       .unionByName(scenario_3_end_df, allowMissingColumns=True) \
                                       .unionByName(scenario_3_new_df, allowMissingColumns=True) \
                                       .unionByName(scenario_6_df, allowMissingColumns=True) \
                                       .unionByName(scenario_7_df, allowMissingColumns=True) \
                                       .unionByName(scenario_8_df, allowMissingColumns=True)

            print(f"Total records to merge: {final_df.count()}")

            if final_df.count() > 0:
                final_df.createOrReplaceTempView("staged_view")

                self.spark.sql(f"""
                    MERGE INTO {self.destination_table} AS dest
                    USING staged_view AS staged
                    ON dest.article_sk = staged.article_sk
                       AND dest.supplier_nbr = staged.supplier_nbr
                       AND dest.user_principal_name = staged.user_principal_name
                       AND dest.start_date = staged.start_date

                    WHEN MATCHED AND staged.end_date IS NOT NULL THEN
                      UPDATE SET end_date = staged.end_date

                    WHEN NOT MATCHED THEN
                      INSERT *
                """)

                print("Data processing completed. Destination table merged successfully.")
            else:
                print("No changes detected. Destination table not updated.")

        except Exception as e:
            print("Error occurred during processing:", e)
            traceback.print_exc(file=sys.stdout)
            sys.exit(1)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Refactored Supplier Article Processor with In-Memory MERGE and Logging").getOrCreate()
    processor = SupplierArticleProcessorSQL(spark)
    processor.process_articles()
