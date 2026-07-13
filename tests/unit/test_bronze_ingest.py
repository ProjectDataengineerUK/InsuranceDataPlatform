from src.common.schemas import CLAIM_EVENT_SCHEMA
from src.streaming.bronze_ingest import DEFAULT_REQUIRED_FIELDS, _is_malformed, _parse_events


def test_malformed_json_is_flagged(spark):
    raw_df = spark.createDataFrame(
        [("k1", "not valid json", 1, 0, 10), ("k2", '{"claim_id": null}', 2, 0, 11)],
        ["key", "value", "timestamp", "partition", "offset"],
    )

    parsed = _parse_events(raw_df, CLAIM_EVENT_SCHEMA)
    flags = {
        row["event_key"]: row["is_malformed"]
        for row in parsed.withColumn(
            "is_malformed", _is_malformed(parsed, DEFAULT_REQUIRED_FIELDS)
        ).collect()
    }

    assert flags["k1"] is True
    assert flags["k2"] is True


def test_valid_event_is_not_flagged(spark):
    valid_json = (
        '{"claim_id": "c1", "policy_id": "p1", "customer_id": "cust1", '
        '"event_type": "claim-opened", "event_timestamp": "2026-01-01T10:00:00", '
        '"amount": 100.0, "region": "SP", "vehicle_type": "carro", "source": "susep"}'
    )
    raw_df = spark.createDataFrame(
        [("k1", valid_json, 1, 0, 10)], ["key", "value", "timestamp", "partition", "offset"]
    )

    parsed = _parse_events(raw_df, CLAIM_EVENT_SCHEMA)
    result = parsed.withColumn(
        "is_malformed", _is_malformed(parsed, DEFAULT_REQUIRED_FIELDS)
    ).collect()

    assert result[0]["is_malformed"] is False


def test_is_malformed_with_empty_required_fields_never_flags_anything(spark):
    # Confirmado contra Spark de verdade (CI): from_json em modo PERMISSIVE
    # (sem columnNameOfCorruptRecord) NUNCA retorna null pro struct inteiro,
    # nem pra JSON totalmente inválido ("not valid json") — só os campos
    # internos ficam null. Ou seja, `col("payload").isNull()` sozinho (o termo
    # base de _is_malformed) não detecta JSON malformado; quem detecta é o
    # check por campo. required_fields=[] portanto NÃO oferece nenhuma
    # proteção contra JSON malformado — não é uma configuração segura pra
    # nenhum job real usar. Por isso regulatory_bronze_ingest sempre passa
    # pelo menos a chave de junção (--required-fields external_reference_id),
    # nunca uma lista vazia; este teste documenta esse limite, não valida um
    # uso de produção.
    raw_df = spark.createDataFrame(
        [("k1", "not valid json", 1, 0, 10), ("k2", '{"claim_id": null}', 2, 0, 11)],
        ["key", "value", "timestamp", "partition", "offset"],
    )

    parsed = _parse_events(raw_df, CLAIM_EVENT_SCHEMA)
    flags = {
        row["event_key"]: row["is_malformed"]
        for row in parsed.withColumn("is_malformed", _is_malformed(parsed, [])).collect()
    }

    assert flags["k1"] is False
    assert flags["k2"] is False
