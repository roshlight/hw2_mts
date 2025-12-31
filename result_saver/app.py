import os
import json
import logging
from confluent_kafka import Consumer
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DB Setup
Base = declarative_base()
class TransactionScore(Base):
    __tablename__ = 'scores'
    id = Column(Integer, primary_key=True)
    transaction_id = Column(String, index=True)
    score = Column(Float)
    fraud_flag = Column(Integer)
    created_at = Column(DateTime, default=func.now())

engine = create_engine(os.getenv("DATABASE_URL"))
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Kafka Setup
conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'group.id': 'result-saver-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['scoring'])

def save_to_db():
    session = Session()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            data_list = json.loads(msg.value().decode('utf-8'))
            # Т.к. pandas.to_json(orient='records') возвращает список
            for data in data_list:
                record = TransactionScore(
                    transaction_id=data['transaction_id'],
                    score=data['score'],
                    fraud_flag=data['fraud_flag']
                )
                session.add(record)
            session.commit()
            logger.info("Saved batch to database")
    except Exception as e:
        logger.error(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    save_to_db()
    