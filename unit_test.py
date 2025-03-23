import pytest
from pyspark.sql import SparkSession
from SupplierArticleSql import SupplierArticleProcessorSQL

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("SupplierArticleTests").getOrCreate()

# Helper to write test data
def write_delta(df, table):
    df.write.format("delta").mode("overwrite").saveAsTable(table)

# Scenario 1 & 4: Multi-supplier - should end-date existing active records
def test_multi_supplier_end(spark):
    source = [(1, 'SUP1', 'userA'), (1, 'SUP2', 'userA')]
    dest = [(1, 'SUP1', 'userA', '2024-01-01', None)]
    write_delta(spark.createDataFrame(source, ['article_sk', 'supplier_nbr', 'user_principal_name']), "test.source")
    write_delta(spark.createDataFrame(dest, ['article_sk', 'supplier_nbr', 'user_principal_name', 'start_date', 'end_date']), "test.dest")

    processor = SupplierArticleProcessorSQL(spark)
    processor.source_table, processor.destination_table = "test.source", "test.dest"
    processor.process_articles()

    result = spark.sql("SELECT * FROM test.dest WHERE end_date IS NOT NULL")
    assert result.count() == 1

# Scenario 2 & 5: Single supplier - insert or reinstate
def test_single_supplier_insert(spark):
    source = [(2, 'SUP3', 'userB')]
    dest = []
    write_delta(spark.createDataFrame(source, ['article_sk', 'supplier_nbr', 'user_principal_name']), "test.source")
    write_delta(spark.createDataFrame(dest, ['article_sk', 'supplier_nbr', 'user_principal_name', 'start_date', 'end_date']), "test.dest")

    processor = SupplierArticleProcessorSQL(spark)
    processor.source_table, processor.destination_table = "test.source", "test.dest"
    processor.process_articles()

    result = spark.sql("SELECT * FROM test.dest WHERE end_date IS NULL")
    assert result.count() == 1

# Scenario 3: Supplier change
def test_supplier_change(spark):
    source = [(3, 'SUP_NEW', 'userC')]
    dest = [(3, 'SUP_OLD', 'userC', '2024-01-01', None)]
    write_delta(spark.createDataFrame(source, ['article_sk', 'supplier_nbr', 'user_principal_name']), "test.source")
    write_delta(spark.createDataFrame(dest, ['article_sk', 'supplier_nbr', 'user_principal_name', 'start_date', 'end_date']), "test.dest")

    processor = SupplierArticleProcessorSQL(spark)
    processor.source_table, processor.destination_table = "test.source", "test.dest"
    processor.process_articles()

    result_ended = spark.sql("SELECT * FROM test.dest WHERE supplier_nbr = 'SUP_OLD' AND end_date IS NOT NULL")
    result_new = spark.sql("SELECT * FROM test.dest WHERE supplier_nbr = 'SUP_NEW' AND end_date IS NULL")
    assert result_ended.count() == 1
    assert result_new.count() == 1

# Scenario 6: New user added
def test_new_user_added(spark):
    source = [(4, 'SUP4', 'userD')]
    dest = []
    write_delta(spark.createDataFrame(source, ['article_sk', 'supplier_nbr', 'user_principal_name']), "test.source")
    write_delta(spark.createDataFrame(dest, ['article_sk', 'supplier_nbr', 'user_principal_name', 'start_date', 'end_date']), "test.dest")

    processor = SupplierArticleProcessorSQL(spark)
    processor.source_table, processor.destination_table = "test.source", "test.dest"
    processor.process_articles()

    result = spark.sql("SELECT * FROM test.dest WHERE article_sk = 4")
    assert result.count() == 1

# Scenario 7: User leaves (should end-date user)
def test_user_leaves(spark):
    source = []  # User is gone
    dest = [(5, 'SUP5', 'userE', '2024-01-01', None)]
    write_delta(spark.createDataFrame(source, ['article_sk', 'supplier_nbr', 'user_principal_name']), "test.source")
    write_delta(spark.createDataFrame(dest, ['article_sk', 'supplier_nbr', 'user_principal_name', 'start_date', 'end_date']), "test.dest")

    processor = SupplierArticleProcessorSQL(spark)
    processor.source_table, processor.destination_table = "test.source", "test.dest"
    processor.process_articles()

    result = spark.sql("SELECT * FROM test.dest WHERE end_date IS NOT NULL")
    assert result.count() == 1

# Scenario 8: Article no longer supplied
def test_article_no_longer_supplied(spark):
    source = []
    dest = [(6, 'SUP6', 'userF', '2024-01-01', None)]
    write_delta(spark.createDataFrame(source, ['article_sk', 'supplier_nbr', 'user_principal_name']), "test.source")
    write_delta(spark.createDataFrame(dest, ['article_sk', 'supplier_nbr', 'user_principal_name', 'start_date', 'end_date']), "test.dest")

    processor = SupplierArticleProcessorSQL(spark)
    processor.source_table, processor.destination_table = "test.source", "test.dest"
    processor.process_articles()

    result = spark.sql("SELECT * FROM test.dest WHERE end_date IS NOT NULL")
    assert result.count() == 1

if __name__ == "__main__":
    spark_session = SparkSession.builder.master("local[*]").appName("SupplierArticleManualTest").getOrCreate()

    test_multi_supplier_end(spark_session)
    test_single_supplier_insert(spark_session)
    test_supplier_change(spark_session)
    test_new_user_added(spark_session)
    test_user_leaves(spark_session)
    test_article_no_longer_supplied(spark_session)

    print("All tests executed successfully.")
