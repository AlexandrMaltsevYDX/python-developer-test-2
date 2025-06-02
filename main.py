from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey
from settings import settings
from datetime import datetime

engine = create_engine(
    settings.DATABASE_URL,
    pool_size=settings.POOL_SIZE,
    max_overflow=settings.MAX_OVERFLOW,
    pool_timeout=settings.POOL_TIMEOUT,
    pool_recycle=settings.POOL_RECYCLE,
    echo=False  # Отключаем логирование SQL запросов для продакшена
)
metadata = MetaData()

users = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('gender', String),
    Column('age', String)
)

heart_rates = Table(
    'heart_rates',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id'), index=True),
    Column('timestamp', DateTime),
    Column('heart_rate', Float),
)

metadata.create_all(engine)

# Вставка тестовых данных в таблицу users
with engine.connect() as conn:
    # Добавляем 2 пользователей
    conn.execute(
        users.insert(),
        [
            {"name": "Иван Петров", "gender": "male", "age": "25"},
            {"name": "Анна Сидорова", "gender": "female", "age": "30"}
        ]
    )
    
    # Добавляем 2 записи пульса
    conn.execute(
        heart_rates.insert(),
        [
            {"user_id": 1, "timestamp": datetime(2024, 1, 15, 10, 30), "heart_rate": 75.5},
            {"user_id": 2, "timestamp": datetime(2024, 1, 15, 11, 15), "heart_rate": 82.3}
        ]
    )
    
    conn.commit()