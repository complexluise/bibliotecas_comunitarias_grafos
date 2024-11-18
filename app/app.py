import streamlit as st
from langchain.chains import GraphCypherQAChain
from langchain_community.graphs import Neo4jGraph
from langchain_openai import ChatOpenAI
import os

st.set_page_config(page_title="🦜🔗 chat bc")
st.title('🦜🔗 Chatea con las Bibliotecas Comunitarias')

# Get environment variables
openai_api_key = os.getenv("OPENAI_API_KEY")
neo4j_uri = os.getenv('NEO4J_URI')
neo4j_user = os.getenv('NEO4J_USER')
neo4j_password = os.getenv('NEO4J_PASSWORD')


def generate_response(question):
    graph = Neo4jGraph(
        url=neo4j_uri,
        username=neo4j_user,
        password=neo4j_password
    )
    chain = GraphCypherQAChain.from_llm(
        ChatOpenAI(
            temperature=0,
            model='gpt-4o-mini-2024-07-18',
            openai_api_key=openai_api_key
        ),
        graph=graph,
        verbose=True
    )
    answer = chain.invoke(question)
    return answer['result']


def save_qa_to_file(question, answer, file_path='qa_history.txt'):
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write(f"Pregunta: {question}\n")
        f.write(f"Respuesta: {answer}\n\n")


with st.form('my_form'):
    question = st.text_area('Ingrese su pregunta:', '¿Cuales son las bibliotecas más diversas en terminos de tipo de población?')
    submitted = st.form_submit_button('Enviar')
    if submitted:
        try:
            response = generate_response(question)
            st.info(response)
            save_qa_to_file(question, response)
        except Exception as e:
            st.error(f"Error al procesar la pregunta: {e}")
