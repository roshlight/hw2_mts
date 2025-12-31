import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
from sqlalchemy import create_engine

# --- КОНФИГУРАЦИЯ ---
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/fraud_db")

# Подключение к базе данных
engine = create_engine(DB_URL)

# --- ФУНКЦИИ ИЗ ТВОЕГО СТАРОГО КОДА ---
def load_file(uploaded_file):
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

# --- ИНТЕРФЕЙС ---
st.set_page_config(page_title="Fraud Detection System", layout="wide")
st.title("Real-Time Fraud Detection System")

# Инициализация состояния (из твоего кода)
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Создаем вкладки для разделения функционала
tab_send, tab_results = st.tabs(["Отправка данных", "Просмотр результатов"])

# --- ВКЛАДКА 1: ТВОЙ СТАРЫЙ КОД ---
with tab_send:
    st.subheader("Симуляция потока транзакций")
    
    uploaded_file = st.file_uploader(
        "Загрузите CSV файл с транзакциями",
        type=["csv"]
    )

    if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": load_file(uploaded_file)
        }
        st.success(f"Файл {uploaded_file.name} успешно загружен!")

    if st.session_state.uploaded_files:
        st.markdown("---")
        for file_name, file_data in st.session_state.uploaded_files.items():
            cols = st.columns([4, 2, 2])
            with cols[0]:
                st.markdown(f"**Файл:** `{file_name}` | **Статус:** `{file_data['status']}`")
            with cols[2]:
                if st.button(f"Отправить в Kafka", key=f"send_{file_name}"):
                    if file_data["df"] is not None:
                        with st.spinner("Отправка в поток..."):
                            success = send_to_kafka(file_data["df"], KAFKA_TOPIC, KAFKA_BROKERS)
                            if success:
                                st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                                st.rerun()

# --- ВКЛАДКА 2: НОВЫЙ КОД ДЛЯ 10 БАЛЛОВ ---
with tab_results:
    st.subheader("Витрина результатов (PostgreSQL)")
    
    if st.button("🔄 Обновить данные из БД"):
        try:
            # 1. Получаем 10 последних фродовых транзакций
            query_fraud = """
                SELECT transaction_id, score, fraud_flag, created_at 
                FROM scores 
                WHERE fraud_flag = 1 
                ORDER BY created_at DESC 
                LIMIT 10
            """
            df_fraud = pd.read_sql(query_fraud, engine)
            
            st.write("### Последние 10 обнаруженных фродов")
            if not df_fraud.empty:
                st.dataframe(df_fraud, use_container_width=True)
            else:
                st.info("Фродовых транзакций пока не найдено.")

            st.markdown("---")

            # 2. Получаем 100 последних скоров для гистограммы
            query_hist = "SELECT score FROM scores ORDER BY created_at DESC LIMIT 100"
            df_hist = pd.read_sql(query_hist, engine)
            
            st.write("### Распределение вероятности фрода (последние 100 транзакций)")
            if not df_hist.empty:
                st.bar_chart(df_hist['score'])
            else:
                st.info("Нет данных для построения гистограммы.")
                
        except Exception as e:
            st.error(f"Ошибка подключения к базе данных: {e}")
            st.info("Убедитесь, что сервис result_saver уже начал записывать данные.")