from datetime import datetime

from src.common.schemas import REGULATORY_CLAIM_RAW_SCHEMA
from src.quality.checks import check_allowed_values
from src.regulatory.standardize import CAUSE_CODE_ALLOWED_VALUES, standardize_regulatory_claims


def _raw_row(**overrides):
    base = {field.name: None for field in REGULATORY_CLAIM_RAW_SCHEMA.fields}
    base.update(overrides)
    return base


def test_standardizes_insurer_a_row(spark):
    raw_df = spark.createDataFrame(
        [
            _raw_row(
                external_reference_id="ref1",
                source_system="insurer_a",
                numero_apolice="123",
                data_ocorrencia="04/05/2020",
                valor_indenizacao="R$ 1.234,56",
                regiao_sinistro="SP",
                codigo_causa="6",
            )
        ],
        schema=REGULATORY_CLAIM_RAW_SCHEMA,
    )

    result = standardize_regulatory_claims(raw_df).collect()[0]

    assert result["policy_id"] == "123"
    assert result["event_timestamp"] == datetime(2020, 5, 4)
    assert float(result["amount"]) == 1234.56
    assert result["region"] == "SP"
    assert result["cause_code"] == 6


def test_standardizes_insurer_b_row(spark):
    raw_df = spark.createDataFrame(
        [
            _raw_row(
                external_reference_id="ref2",
                source_system="insurer_b",
                POLICY_NUM="456",
                EVENT_DATE="2020-06-01",
                CLAIM_AMOUNT="1500.0",
                REGION_CODE="RJ",
                CAUSE_CD="4",
            )
        ],
        schema=REGULATORY_CLAIM_RAW_SCHEMA,
    )

    result = standardize_regulatory_claims(raw_df).collect()[0]

    assert result["policy_id"] == "456"
    assert result["event_timestamp"] == datetime(2020, 6, 1)
    assert float(result["amount"]) == 1500.0
    assert result["region"] == "RJ"
    assert result["cause_code"] == 4


def test_standardizes_insurer_c_row(spark):
    # from_json real (produção) serializa inteiros grandes em StringType usando
    # notação científica (ex.: "1.5935616E12") — usar essa forma aqui, não
    # str(epoch_millis) puro, é o que expôs o bug real de CAST_INVALID_INPUT
    # (cast("long") direto rejeita notação científica; cast("double") primeiro
    # não).
    epoch_millis = int(datetime(2020, 7, 1).timestamp() * 1000)
    raw_df = spark.createDataFrame(
        [
            _raw_row(
                external_reference_id="ref3",
                source_system="insurer_c",
                policyId="789",
                occurrenceDate=f"{float(epoch_millis):E}",
                amountCents=f"{150000.0:E}",
                regionCode="MG",
                causeCode="9",
            )
        ],
        schema=REGULATORY_CLAIM_RAW_SCHEMA,
    )

    result = standardize_regulatory_claims(raw_df).collect()[0]

    assert result["policy_id"] == "789"
    assert result["event_timestamp"] == datetime(2020, 7, 1)
    assert float(result["amount"]) == 1500.0
    assert result["region"] == "MG"
    assert result["cause_code"] == 9


def test_invalid_cause_code_caught_by_allowed_values_check(spark):
    raw_df = spark.createDataFrame(
        [
            _raw_row(
                external_reference_id="ref4",
                source_system="insurer_b",
                POLICY_NUM="999",
                EVENT_DATE="2020-06-01",
                CLAIM_AMOUNT="100.0",
                REGION_CODE="RJ",
                CAUSE_CD="99",
            )
        ],
        schema=REGULATORY_CLAIM_RAW_SCHEMA,
    )

    standardized_df = standardize_regulatory_claims(raw_df)
    result = check_allowed_values(
        standardized_df, "cause_code", CAUSE_CODE_ALLOWED_VALUES, "silver.regulatory_claims"
    )

    assert result.passed is False
    assert result.failed_rows == 1
