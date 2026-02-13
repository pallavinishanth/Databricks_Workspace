import os
import streamlit as st
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# --- Configuration ---
INDEX_NAME = os.environ["VS_INDEX_FULL_NAME"] 
LLM_ENDPOINT_NAME = os.environ["LLM_MODEL_NAME"] 
EMBEDDING_MODEL_ENDPOINT_NAME = "databricks-gte-large-en" # Must match model used for indexing

# --- Initialize Clients ---
w = WorkspaceClient()

# --- RAG Functions ---

def get_embeddings(text):
    """Generates embeddings for the query using the Databricks Foundation Model API."""
    try:
        response = w.serving_endpoints.get_open_ai_client().embeddings.create(
            model=EMBEDDING_MODEL_ENDPOINT_NAME,
            input=text,
        )
        return response.data[0].embedding
    except Exception as e:
        st.error(f"Error generating embeddings: {e}")
        return None

def retrieve_context(query_text: str):
    """Queries the Databricks Vector Search index for relevant documents."""
    query_vector = get_embeddings(query_text)
    if query_vector is None:
        return []

    index = w.vector_search_indexes.get_index(index_name=INDEX_NAME) # Replace YOUR_VECTOR_SEARCH_ENDPOINT_NAME
    
    # Define columns to fetch (must exist in your index)
    columns_to_fetch = ["combined_text"] 

    try:
        # Perform similarity search
        search_results = w.vector_search_indexes.query_index(
            index_name=INDEX_NAME,
            query_vector=query_vector,
            columns=columns_to_fetch,
            num_results=20 # Adjust based on your needs most common is top-k value
        )
        # Extract content from results
        cols = [c.name for c in search_results.manifest.columns]
        rows = [dict(zip(cols, r)) for r in (search_results.result.data_array or [])]

        # print the retreived data for testing
        # st.write(f"Retrieved {len(rows)} rows")
        # st.json(rows[:3]) 

        context = "\n".join(
            row["combined_text"]
            for row in rows
            if row.get("combined_text")
        )
        return context

    except Exception as e:
        st.error(f"Error during vector search: {e}")
        return ""

def generate_response(prompt_with_context: str):
    """Generates a response using the Databricks Foundation Model API."""
    try:
        # Uses a unified OpenAI-compatible API for foundation models
        response = w.serving_endpoints.get_open_ai_client().chat.completions.create(
            model=LLM_ENDPOINT_NAME,
            messages=[{"role": "user", "content": prompt_with_context}],
            max_tokens=512,
        )
        return response.choices[0].message.content
    except Exception as e:
        st.error(f"Error generating LLM response: {e}")
        return ""

# --- Streamlit App ---

st.set_page_config(page_title="Databricks RAG Health Chatbot", page_icon="🧠", layout="centered")
st.title("🧠 Patient Record Chatbot")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Ask a question about your data..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # RAG workflow
    context = retrieve_context(prompt)
    
    if context:
        # Augment the prompt with retrieved context
        rag_prompt_template = ChatPromptTemplate.from_messages([
            ("system", "You are a helpful assistant. Use the following context to answer the user's question: {context}"),
            ("human", "{question}")
        ])
        
        prompt_value = rag_prompt_template.invoke({"context": context, "question": prompt})
        prompt_with_context = prompt_value.to_string()
        
        response = generate_response(prompt_with_context)

    else:
        response = "Sorry, I couldn't find relevant information in the knowledge base."

    with st.chat_message("assistant"):
        st.markdown(response)
    st.session_state.messages.append({"role": "assistant", "content": response})

