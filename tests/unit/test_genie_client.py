import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "apps" / "insurance_platform_app"))

from genie_client import _extract_answer, _message_id, is_configured  # noqa: E402


class _FakeText:
    def __init__(self, content):
        self.content = content


class _FakeQuery:
    def __init__(self, query, description):
        self.query = query
        self.description = description


class _FakeAttachment:
    def __init__(self, attachment_id, text=None, query=None):
        self.attachment_id = attachment_id
        self.text = text
        self.query = query


class _FakeMessage:
    def __init__(self, attachments):
        self.attachments = attachments


def test_is_configured_reflects_env_var(monkeypatch):
    monkeypatch.delenv("GENIE_SPACE_ID", raising=False)
    assert is_configured() is False

    monkeypatch.setenv("GENIE_SPACE_ID", "some-space-id")
    assert is_configured() is True


def test_extract_answer_text_only():
    message = _FakeMessage([_FakeAttachment("a1", text=_FakeText("42 sinistros fora do SLA."))])

    result = _extract_answer(message)

    assert result["text"] == "42 sinistros fora do SLA."
    assert result["sql"] is None
    assert result["query_attachment_id"] is None


def test_extract_answer_text_and_query():
    message = _FakeMessage(
        [
            _FakeAttachment("a1", text=_FakeText("Aqui está a contagem por região.")),
            _FakeAttachment(
                "a2",
                query=_FakeQuery("SELECT region, COUNT(*) FROM gold.claims GROUP BY region", "contagem por região"),
            ),
        ]
    )

    result = _extract_answer(message)

    assert result["text"] == "Aqui está a contagem por região."
    assert "GROUP BY region" in result["sql"]
    assert result["sql_description"] == "contagem por região"
    assert result["query_attachment_id"] == "a2"


def test_extract_answer_no_attachments():
    result = _extract_answer(_FakeMessage(None))

    assert result == {
        "text": None,
        "sql": None,
        "sql_description": None,
        "query_attachment_id": None,
    }


class _MessageWithoutMessageId:
    # Simula a versão do databricks-sdk instalada em produção, onde
    # GenieMessage não tem o atributo message_id ainda (AttributeError
    # confirmado em produção) — só o campo legado "id".
    def __init__(self, legacy_id):
        self.id = legacy_id


class _MessageWithMessageId:
    def __init__(self, message_id, legacy_id):
        self.message_id = message_id
        self.id = legacy_id


def test_message_id_falls_back_to_legacy_id_field():
    assert _message_id(_MessageWithoutMessageId("legacy-123")) == "legacy-123"


def test_message_id_prefers_canonical_field_when_present():
    assert _message_id(_MessageWithMessageId("new-456", "legacy-456")) == "new-456"


def test_message_id_returns_none_when_neither_field_exists():
    assert _message_id(object()) is None
