from src.quality.checks import check_not_null, check_range, check_unique


def test_check_not_null_detects_nulls(spark):
    df = spark.createDataFrame(
        [("c1", 100.0), ("c2", None), (None, 50.0)],
        ["claim_id", "amount"],
    )

    results = check_not_null(df, ["claim_id", "amount"], "silver.claims")

    by_check = {r.check_name: r for r in results}
    assert by_check["not_null:claim_id"].passed is False
    assert by_check["not_null:claim_id"].failed_rows == 1
    assert by_check["not_null:amount"].passed is False
    assert by_check["not_null:amount"].failed_rows == 1


def test_check_not_null_passes_when_no_nulls(spark):
    df = spark.createDataFrame([("c1", 100.0), ("c2", 200.0)], ["claim_id", "amount"])

    results = check_not_null(df, ["claim_id"], "silver.claims")

    assert all(r.passed for r in results)
    assert results[0].failed_rows == 0


def test_check_unique_detects_duplicates(spark):
    df = spark.createDataFrame(
        [("c1",), ("c1",), ("c2",)],
        ["claim_id"],
    )

    result = check_unique(df, ["claim_id"], "silver.claims")

    assert result.passed is False
    assert result.failed_rows == 1


def test_check_range_detects_out_of_range_values(spark):
    df = spark.createDataFrame([(10.0,), (-5.0,), (999999.0,)], ["amount"])

    result = check_range(df, "amount", min_value=0.0, max_value=100000.0, table_name="gold.claims")

    assert result.passed is False
    assert result.failed_rows == 2
