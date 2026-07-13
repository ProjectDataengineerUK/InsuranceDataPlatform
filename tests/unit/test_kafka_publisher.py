import json

import pytest

from src.ingestion.producer.kafka_publisher import publish_events


class _FakeProducer:
    def __init__(self):
        self.produced = []

    def produce(self, topic, key, value, callback=None):
        self.produced.append((topic, key, value))

    def poll(self, timeout):
        return 0

    def flush(self):
        pass


def test_publish_events_raises_when_no_events():
    producer = _FakeProducer()

    with pytest.raises(ValueError):
        list(publish_events(producer, topic="claim-opened", events=[], events_per_minute=100))


def test_publish_events_stops_gracefully_after_max_duration(monkeypatch):
    monkeypatch.setattr("src.ingestion.producer.kafka_publisher.time.sleep", lambda _: None)

    clock = {"now": 0.0}

    def fake_monotonic():
        clock["now"] += 0.5
        return clock["now"]

    monkeypatch.setattr("src.ingestion.producer.kafka_publisher.time.monotonic", fake_monotonic)

    producer = _FakeProducer()
    events = [{"claim_id": "c1"}, {"claim_id": "c2"}, {"claim_id": "c3"}]

    consumed = list(
        publish_events(
            producer,
            topic="claim-opened",
            events=events,
            events_per_minute=6000,
            shuffle=False,
            loop=True,
            max_duration_seconds=1.0,
        )
    )

    # loop=True faria isso rodar pra sempre sem o corte de duração — o teste
    # existe justamente pra travar que o processo agendado no GitHub Actions
    # (producer.yml) encerra sozinho em vez de depender de kill forçado.
    assert 0 < len(consumed) < len(events) * 5
    assert len(producer.produced) == len(consumed)


def test_publish_events_serializes_nan_as_json_null():
    # pd.DataFrame(rows) força None -> NaN em colunas numéricas mistas (ex.:
    # CLAIM_AMOUNT/amountCents em regulatory_feeds.py, simulando campo
    # ausente) — json.dumps de um float NaN vira o token inválido "NaN", que
    # o from_json/cast do lado Spark não trata como ausente (confirmado em
    # produção: CAST_INVALID_INPUT em standardize.py). Deve sempre virar
    # JSON null.
    producer = _FakeProducer()
    events = [{"policy_id": "p1", "CLAIM_AMOUNT": float("nan")}]

    list(
        publish_events(
            producer,
            topic="regulatory-claim-report",
            events=events,
            events_per_minute=6000,
            shuffle=False,
            loop=False,
        )
    )

    _, _, value = producer.produced[0]
    payload = json.loads(value)
    assert payload["CLAIM_AMOUNT"] is None
    assert "NaN" not in value.decode("utf-8")
