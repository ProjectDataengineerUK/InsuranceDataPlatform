import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.service.dashboards import GenieMessage


def is_configured() -> bool:
    return bool(os.environ.get("GENIE_SPACE_ID"))


def _extract_answer(message: "GenieMessage") -> dict:
    # Um GenieMessage pode ter vários attachments (texto + SQL + follow-up
    # questions no mesmo turno) — concatena todo texto e guarda só o último
    # attachment de query, que é o caso comum (Genie não costuma gerar mais
    # de uma consulta por resposta).
    text_parts = []
    sql_query = None
    sql_description = None
    query_attachment_id = None

    for attachment in message.attachments or []:
        if attachment.text and attachment.text.content:
            text_parts.append(attachment.text.content)
        if attachment.query:
            sql_query = attachment.query.query
            sql_description = attachment.query.description
            query_attachment_id = attachment.attachment_id

    return {
        "text": "\n\n".join(text_parts) if text_parts else None,
        "sql": sql_query,
        "sql_description": sql_description,
        "query_attachment_id": query_attachment_id,
    }


def ask_genie(content: str, conversation_id: str | None = None) -> dict:
    """Envia uma pergunta em linguagem natural pro Genie Space configurado via
    GENIE_SPACE_ID (ver resources/visualization.yml + app.yaml). A restrição de
    escopo — quais tabelas o Genie enxerga — é controlada inteiramente pela
    configuração do próprio Genie Space na UI do Databricks, não por este
    código: este client só encaminha a pergunta e traduz a resposta pro
    formato que pages/chat.py consome.

    Import do databricks.sdk fica dentro da função (não no topo do módulo),
    mesmo padrão de queries.py::get_connection — permite importar este módulo
    e testar _extract_answer sem o pacote databricks-sdk instalado.
    """
    from databricks.sdk import WorkspaceClient

    space_id = os.environ["GENIE_SPACE_ID"]
    w = WorkspaceClient()

    if conversation_id:
        message = w.genie.create_message_and_wait(
            space_id=space_id, conversation_id=conversation_id, content=content
        )
    else:
        message = w.genie.start_conversation_and_wait(space_id=space_id, content=content)

    result: dict = {
        "conversation_id": message.conversation_id,
        "message_id": message.message_id,
        "error": message.error.error if message.error else None,
        "rows": None,
        "columns": None,
    }
    result.update(_extract_answer(message))

    if result["query_attachment_id"]:
        try:
            query_result = w.genie.get_message_attachment_query_result(
                space_id=space_id,
                conversation_id=message.conversation_id,
                message_id=message.message_id,
                attachment_id=result["query_attachment_id"],
            )
            statement = query_result.statement_response
            if statement and statement.result and statement.manifest and statement.manifest.schema:
                columns = [col.name for col in statement.manifest.schema.columns or []]
                data = statement.result.data_array or []
                result["columns"] = columns
                result["rows"] = [dict(zip(columns, row, strict=False)) for row in data]
        except Exception as exc:  # noqa: BLE001
            result["query_result_error"] = str(exc)

    return result
