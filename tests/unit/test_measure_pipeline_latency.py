from scripts.measure_pipeline_latency import measure_throughput


def test_measure_throughput_within_sla_none_when_no_data(spark):
    empty_df = spark.createDataFrame([], "event_key string, _ingested_at timestamp")
    empty_df.createOrReplaceTempView("bronze_test_empty")

    result = measure_throughput(spark, "bronze_test_empty", window_minutes=15)

    assert result["minutes_observed"] == 0
    assert result["within_sla"] is None


def test_measure_throughput_within_sla_false_when_below_expected(spark):
    df = spark.sql("SELECT 'k1' AS event_key, current_timestamp() AS _ingested_at")
    df.createOrReplaceTempView("bronze_test_low_volume")

    result = measure_throughput(spark, "bronze_test_low_volume", window_minutes=15)

    # 1 evento no minuto observado, muito abaixo de 80% dos 100 esperados.
    assert result["minutes_observed"] == 1
    assert result["within_sla"] is False
