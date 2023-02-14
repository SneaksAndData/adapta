from proteus.storage.models.hive import HivePath

def test_from_hdfs_path():
    hive_path = HivePath.from_hdfs_path("hive://sqlserver@servername.database.windows.net:1433/database/schema/table")
    assert hive_path.hive_engine == "sqlserver"
    assert hive_path.hive_server == "servername.database.windows.net"
    assert hive_path.hive_server_port == 1433
    assert hive_path.hive_database == "database"
    assert hive_path.hive_schema == "schema"
    assert hive_path.hive_table == "table"
    