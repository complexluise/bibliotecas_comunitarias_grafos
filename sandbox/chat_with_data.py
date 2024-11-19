import os

from langchain.chains import GraphCypherQAChain
from langchain_community.graphs import Neo4jGraph
from langchain_openai import ChatOpenAI


def save_qa_to_file(question, answer, file_path):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(f"Pregunta: {question}\n")
        f.write(f"Respuesta: {answer}\n\n")


def get_user_input():
    return input("Ingrese su pregunta (o 'salir' para terminar): ")


def main():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Error: No se encontró la clave API de OpenAI.")
        return

    graph = Neo4jGraph(
        url=os.getenv("NEO4J_URI"),
        username=os.getenv("NEO4J_USER"),
        password=os.getenv("NEO4J_PASSWORD"),
        enhanced_schema=True,
    )

    chain = GraphCypherQAChain.from_llm(
        ChatOpenAI(temperature=0, model="gpt-4o-mini-2024-07-18"),
        graph=graph,
        verbose=True,
    )

    qa_file = "qa_history.txt"

    while True:
        question = get_user_input()
        if question.lower() == "salir":
            break

        try:
            answer = chain.invoke(question)
            print(f"Respuesta: {answer['result']}")
            save_qa_to_file(question, answer["result"], qa_file)
        except Exception as e:
            print(f"Error al procesar la pregunta: {e}")

    print(f"Las preguntas y respuestas han sido guardadas en {qa_file}")


if __name__ == "__main__":

    main()
