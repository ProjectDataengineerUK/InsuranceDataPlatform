from src.common.delta_write import append_or_create


def test_append_or_create_creates_table_when_absent(spark):
    df = spark.createDataFrame([("f1", 1.0)], ["feature_name", "value"])

    append_or_create(df, "delta_write_test_create")

    result = spark.read.table("delta_write_test_create").collect()
    assert len(result) == 1
    assert result[0]["feature_name"] == "f1"


def test_append_or_create_appends_to_existing_table(spark):
    first = spark.createDataFrame([("f1", 1.0)], ["feature_name", "value"])
    append_or_create(first, "delta_write_test_append")

    second = spark.createDataFrame([("f2", 2.0)], ["feature_name", "value"])
    append_or_create(second, "delta_write_test_append")

    rows = {r["feature_name"]: r["value"] for r in spark.read.table("delta_write_test_append").collect()}
    assert rows == {"f1": 1.0, "f2": 2.0}
