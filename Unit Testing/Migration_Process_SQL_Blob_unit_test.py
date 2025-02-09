import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from data_migration import DataMigration

class TestDataMigration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("UnitTest").getOrCreate()
        cls.data_migration = DataMigration()

    @patch('data_migration.dbutils')
    def test_set_storage_account(self, mock_dbutils):
        mock_dbutils.secrets.get.return_value = "dummy_key"
        self.data_migration.set_storage_account()
        config_value = self.spark.conf.get(f"fs.azure.account.key.{self.data_migration.delta_account_name}.dfs.core.windows.net")
        self.assertEqual(config_value, "dummy_key")

    @patch('data_migration.DataMigration.read_data_from_sqlmi')
    def test_read_data_from_sqlmi(self, mock_read):
        mock_df = self.spark.createDataFrame([(1, 'test')], ["id", "name"])
        mock_read.return_value = mock_df
        df = self.data_migration.read_data_from_sqlmi("test_table")
        self.assertEqual(df.count(), 1)

    @patch('data_migration.DataMigration.get_max_value')
    def test_get_max_value(self, mock_get_max_value):
        mock_get_max_value.return_value = 100
        max_value = self.data_migration.get_max_value("test_table", "id")
        self.assertEqual(max_value, 100)

    @patch('data_migration.DataMigration.read_incremental_data_from_sqlmi')
    def test_read_incremental_data_from_sqlmi(self, mock_read_incremental):
        mock_df = self.spark.createDataFrame([(101, 'new_data')], ["id", "name"])
        mock_read_incremental.return_value = mock_df
        df = self.data_migration.read_incremental_data_from_sqlmi("dbo", "test_table", "id", 100)
        self.assertEqual(df.count(), 1)

    @patch('data_migration.DataMigration.write_overwrite')
    def test_write_overwrite(self, mock_write):
        mock_df = self.spark.createDataFrame([(1, 'test')], ["id", "name"])
        self.data_migration.write_overwrite(mock_df, "dummy_path")
        mock_write.assert_called_once()

    @patch('data_migration.DataMigration.process_full_table')
    def test_process_full_table(self, mock_process_full):
        self.data_migration.process_full_table("dbo", "dim_table", "dummy_path")
        mock_process_full.assert_called_once()

    @patch('data_migration.DataMigration.process_incremental_table')
    def test_process_incremental_table(self, mock_process_incremental):
        self.data_migration.process_incremental_table("dbo", "fact_table", "dummy_path", "datekey", "last_updated")
        mock_process_incremental.assert_called_once()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
