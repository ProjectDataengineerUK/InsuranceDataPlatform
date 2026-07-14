import streamlit as st
from genie_client import ask_genie, is_configured

st.title("Chat com os dados")
st.caption(
    "Pergunte em linguagem natural — conversa com o Genie Space 'Insurance "
    "Claims and Compliance Analytics' (Databricks AI/BI). O escopo de tabelas "
    "que o Genie enxerga é definido na configuração do próprio Space na UI do "
    "Databricks, não neste código — mantenha restrito a gold/monitoring lá, "
    "não a bronze/silver com dado bruto."
)

if not is_configured():
    st.info(
        "Não configurado — requer GENIE_SPACE_ID (ver resources/visualization.yml "
        "e app.yaml). Ver docs/ARCHITECTURE.md."
    )
    st.stop()

if "genie_conversation_id" not in st.session_state:
    st.session_state.genie_conversation_id = None
if "genie_messages" not in st.session_state:
    st.session_state.genie_messages = []

for message in st.session_state.genie_messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message.get("sql"):
            with st.expander("Ver SQL gerado"):
                st.code(message["sql"], language="sql")
        if message.get("rows"):
            st.dataframe(message["rows"], use_container_width=True)

question = st.chat_input("Pergunte sobre sinistros, fraude, conformidade SUSEP...")

if question:
    st.session_state.genie_messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        try:
            with st.spinner("Consultando o Genie..."):
                result = ask_genie(question, conversation_id=st.session_state.genie_conversation_id)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Erro ao consultar o Genie: {exc}")
            st.stop()

        st.session_state.genie_conversation_id = result["conversation_id"]

        if result.get("error"):
            answer_text = f"Não consegui responder: {result['error']}"
        else:
            answer_text = result.get("text") or "Sem resposta em texto — ver SQL/resultado abaixo."

        st.markdown(answer_text)
        assistant_message = {"role": "assistant", "content": answer_text}

        if result.get("sql"):
            with st.expander("Ver SQL gerado"):
                st.code(result["sql"], language="sql")
            assistant_message["sql"] = result["sql"]

        if result.get("rows"):
            st.dataframe(result["rows"], use_container_width=True)
            assistant_message["rows"] = result["rows"]
        elif result.get("query_result_error"):
            st.warning(
                f"Consulta gerada, mas não consegui buscar o resultado: {result['query_result_error']}"
            )

        st.session_state.genie_messages.append(assistant_message)

st.divider()
if st.button("Nova conversa"):
    st.session_state.genie_conversation_id = None
    st.session_state.genie_messages = []
    st.rerun()
