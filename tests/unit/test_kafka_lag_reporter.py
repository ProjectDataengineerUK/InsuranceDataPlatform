from src.streaming.kafka_lag_reporter import report_offsets


class _FakeFuture:
    def __init__(self, exc=None):
        self._exc = exc

    def result(self):
        if self._exc:
            raise self._exc


class _FakeAdminClient:
    def __init__(self, config):
        self.config = config
        self.calls = []

    def alter_consumer_group_offsets(self, requests):
        self.calls.append(requests)
        return {i: _FakeFuture() for i in range(len(requests))}


def test_report_offsets_does_nothing_when_no_partitions(monkeypatch):
    created = []
    monkeypatch.setattr(
        "src.streaming.kafka_lag_reporter.AdminClient",
        lambda config: created.append(config) or _FakeAdminClient(config),
    )

    report_offsets("claim-opened", "databricks-bronze-claim-opened", {})

    assert created == []


def test_report_offsets_commits_max_offset_plus_one(monkeypatch):
    fake_admin = _FakeAdminClient({})
    monkeypatch.setattr(
        "src.streaming.kafka_lag_reporter.AdminClient", lambda config: fake_admin
    )
    monkeypatch.setattr(
        "src.streaming.kafka_lag_reporter.get_admin_client_config", lambda: {}
    )

    report_offsets("claim-opened", "databricks-bronze-claim-opened", {0: 99, 1: 5})

    assert len(fake_admin.calls) == 1
    cgtp = fake_admin.calls[0][0]
    assert cgtp.group_id == "databricks-bronze-claim-opened"
    offsets = {tp.partition: tp.offset for tp in cgtp.topic_partitions}
    assert offsets == {0: 100, 1: 6}


def test_report_offsets_never_raises_on_admin_failure(monkeypatch):
    class _RaisingAdminClient:
        def __init__(self, config):
            pass

        def alter_consumer_group_offsets(self, requests):
            raise RuntimeError("cluster unreachable")

    monkeypatch.setattr(
        "src.streaming.kafka_lag_reporter.AdminClient", _RaisingAdminClient
    )

    report_offsets("claim-opened", "databricks-bronze-claim-opened", {0: 1})
