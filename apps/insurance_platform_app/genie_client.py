import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.service.dashboards import GenieMessage


def is_configured() -> bool:
    return bool(os.environ.get("GENIE_SPACE_ID"))


def _message_id(message: "GenieMessage") -> str | None:
    # message_id é o campo canônico, mas versões mais antigas do
    # databricks-sdk instalado neste workspace não têm esse atributo no
    # dataclass (AttributeError confirmado em produção) — .id é o
    # identificador "legado" que a própria SDK documenta como equivalente,
    # ainda presente nessas versões.
    return getattr(message, "message_id", None) or getattr(message, "id", None)


def _extract_answer(message: "GenieMessage") -> dict:
    # Um GenieMessage pode ter vários attachments (texto + SQL + follow-up
    # questions no mesmo turno) — concatena todo texto e guarda só o último
    # attachment de query, que é o caso comum (Genie não costuma gerar mais
    # de uma consulta por resposta).
    #
    # getattr(..., None) em CADA campo, não só nos IDs: a versão do
    # databricks-sdk instalada neste workspace já se mostrou mais antiga do
    # que os dataclasses documentados hoje em pelo menos 2 campos
    # (GenieMessage.message_id, GenieAttachment.attachment_id, ambos
    # confirmados via AttributeError em produção) — mais divergências de
    # schema são prováveis, então nenhum atributo aqui é assumido presente.
    text_parts = []
    sql_query = None
    sql_description = None
    query_attachment_id = None

    for attachment in message.attachments or []:
        text_attachment = getattr(attachment, "text", None)
        content = getattr(text_attachment, "content", None) if text_attachment else None
        if content:
            text_parts.append(content)

        query_attachment = getattr(attachment, "query", None)
        if query_attachment:
            sql_query = getattr(query_attachment, "query", None)
            sql_description = getattr(query_attachment, "description", None)
            query_attachment_id = getattr(attachment, "attachment_id", None) or getattr(
                query_attachment, "id", None
            )

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

    message_id = _message_id(message)
    result: dict = {
        "conversation_id": message.conversation_id,
        "message_id": message_id,
        "error": message.error.error if message.error else None,
        "rows": None,
        "columns": None,
    }
    result.update(_extract_answer(message))

    if result["sql"]:
        try:
            if result["query_attachment_id"]:
                query_result = w.genie.get_message_attachment_query_result(
                    space_id=space_id,
                    conversation_id=message.conversation_id,
                    message_id=message_id,
                    attachment_id=result["query_attachment_id"],
                )
            else:
                # Sem attachment_id (versão antiga do SDK/API, sem o
                # attachment-based endpoint) — get_message_query_result é o
                # equivalente pré-attachment, sem esse parâmetro extra.
                query_result = w.genie.get_message_query_result(
                    space_id=space_id, conversation_id=message.conversation_id, message_id=message_id
                )

            statement = getattr(query_result, "statement_response", None)
            result_data = getattr(statement, "result", None) if statement else None
            manifest = getattr(statement, "manifest", None) if statement else None
            schema = getattr(manifest, "schema", None) if manifest else None
            if result_data and schema:
                columns = [col.name for col in getattr(schema, "columns", None) or []]
                data = getattr(result_data, "data_array", None) or []
                result["columns"] = columns
                result["rows"] = [dict(zip(columns, row, strict=False)) for row in data]
        except Exception as exc:  # noqa: BLE001
            result["query_result_error"] = str(exc)

    return result
