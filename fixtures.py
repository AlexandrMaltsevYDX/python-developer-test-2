from sqlalchemy import Engine
import random


from datetime import datetime, timedelta

import os
from dataclasses import dataclass
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey, Index, text

load_dotenv()

@dataclass
class Settings:
    DATABASE_URL: str = "postgresql+psycopg://user:password@localhost:5432/dbname"
    POOL_SIZE: int = 20
    MAX_OVERFLOW: int = 30
    POOL_TIMEOUT: int = 30
    POOL_RECYCLE: int = 3600
    
    @classmethod
    def from_env(cls) -> 'Settings':
        """Загрузка настроек из переменных окружения"""
        return cls(
            DATABASE_URL=os.getenv('DATABASE_URL'),
            POOL_SIZE=int(os.getenv('POOL_SIZE', cls.POOL_SIZE)),
            MAX_OVERFLOW=int(os.getenv('MAX_OVERFLOW', cls.MAX_OVERFLOW)),
            POOL_TIMEOUT=int(os.getenv('POOL_TIMEOUT', cls.POOL_TIMEOUT)),
            POOL_RECYCLE=int(os.getenv('POOL_RECYCLE', cls.POOL_RECYCLE)),
        )

# Использование
settings = Settings.from_env()
engine = create_engine(
    settings.DATABASE_URL,
    pool_size=settings.POOL_SIZE,
    max_overflow=settings.MAX_OVERFLOW,
    pool_timeout=settings.POOL_TIMEOUT,
    pool_recycle=settings.POOL_RECYCLE,
    echo=False  # Отключаем логирование SQL запросов для продакшена
)
metadata = MetaData()


# Дублирования кода исколючительно для целей тестирования, и для удобства проверки
users = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('gender', String),
    Column('age', Integer) # Change to Integer
)

heart_rates = Table(
    'heart_rates',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id'), index=True),
    Column('timestamp', DateTime),
    Column('heart_rate', Float),
)

# Создаем индексы для оптимизации запросов
# Index('idx_users_age_gender', users.c.age, users.c.gender)  # Для фильтрации пользователей
# Index('idx_users_gender_age', users.c.gender, users.c.age)  # Альтернативный порядок для разных запросов
# Index('idx_heart_rates_timestamp', heart_rates.c.timestamp)  # Для фильтрации по времени
# Index('idx_heart_rates_timestamp_user', heart_rates.c.timestamp, heart_rates.c.user_id)  # Составной для JOIN + время
# Index('idx_heart_rates_user_timestamp', heart_rates.c.user_id, heart_rates.c.timestamp)  # Для агрегации по пользователю


def load_large_fixtures(engine: Engine, users_count: int = 10000, target_heart_rate_records: int = 2500000):
    """
    Создает большой объем тестовых данных для нагрузочного тестирования.
    
    Args:
        engine: SQLAlchemy engine
        users_count: количество пользователей (по умолчанию 10,000)
        target_heart_rate_records: целевое количество записей пульса (по умолчанию 2.5М)
    """
    print(f"🚀 Начинаем генерацию {users_count:,} пользователей и ~{target_heart_rate_records:,} записей пульса...")
    
    # 1. Генерация пользователей
    print("👥 Создание пользователей...")
    _generate_users_batch(engine, users_count)
    
    # 2. Генерация записей пульса
    print("💓 Создание записей пульса...")
    _generate_heart_rates_batch(engine, users_count, target_heart_rate_records)
    
    print("✅ Генерация данных завершена!")

def _generate_users_batch(engine: Engine, users_count: int):
    """Генерация пользователей батчами для эффективности"""
    
    # Списки для генерации реалистичных имен
    first_names_male = ["John", "Mike", "David", "Chris", "Alex", "Daniel", "James", "Robert", "William", "Thomas"]
    first_names_female = ["Anna", "Sarah", "Emma", "Lisa", "Kate", "Maria", "Jennifer", "Jessica", "Ashley", "Amanda"]
    last_names = ["Smith", "Johnson", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", 
                  "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez"]
    
    batch_size = 10000
    total_batches = (users_count + batch_size - 1) // batch_size
    
    with engine.connect() as conn:
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, users_count)
            batch_data = []
            
            for i in range(start_idx, end_idx):
                # Реалистичное распределение по полу и возрасту
                gender = random.choice(['male', 'female'])
                age = _generate_realistic_age()
                
                # Генерируем имя
                if gender == 'male':
                    first_name = random.choice(first_names_male)
                else:
                    first_name = random.choice(first_names_female)
                
                last_name = random.choice(last_names)
                name = f"{first_name} {last_name}"
                
                batch_data.append({
                    "name": name,
                    "gender": gender,
                    "age": age
                })
            
            # Вставляем батч
            conn.execute(users.insert(), batch_data)
            
            if batch_num % 10 == 0:  # Прогресс каждые 10 батчей
                progress = (batch_num + 1) / total_batches * 100
                print(f"  👥 Прогресс пользователей: {progress:.1f}% ({end_idx:,}/{users_count:,})")
        
        conn.commit()
    
    print(f"✅ Создано {users_count:,} пользователей")

def _generate_heart_rates_batch(engine: Engine, users_count: int, target_records: int):
    """Генерация записей пульса с реалистичным распределением"""
    
    # Параметры временного распределения
    start_date = datetime(2023, 1, 1)  # Год данных
    end_date = datetime(2024, 1, 1)
    total_days = (end_date - start_date).days
    
    # Вычисляем среднее количество записей на пользователя
    avg_records_per_user = target_records // users_count
    print(f"📊 Среднее записей на пользователя: {avg_records_per_user}")
    
    # Создаем профили активности пользователей
    user_profiles = _generate_user_activity_profiles(users_count, avg_records_per_user)
    
    batch_size = 10000  # Записей в одном батче
    current_batch = []
    total_generated = 0
    
    with engine.connect() as conn:
        for user_id in range(1, users_count + 1):
            records_for_user = user_profiles[user_id - 1]
            
            # Генерируем записи для этого пользователя
            user_records = _generate_user_heart_rate_records(
                user_id, records_for_user, start_date, total_days
            )
            
            current_batch.extend(user_records)
            total_generated += len(user_records)
            
            # Вставляем батч когда он заполнится
            if len(current_batch) >= batch_size:
                conn.execute(heart_rates.insert(), current_batch)
                current_batch = []
                
                # Показываем прогресс
                progress = total_generated / target_records * 100
                print(f"  💓 Прогресс записей: {progress:.1f}% ({total_generated:,}/{target_records:,})")
        
        # Вставляем оставшиеся записи
        if current_batch:
            conn.execute(heart_rates.insert(), current_batch)
        
        conn.commit()
    
    print(f"✅ Создано {total_generated:,} записей пульса")
    return total_generated

def _generate_realistic_age():
    """Генерация реалистичного возраста с нормальным распределением"""
    # Средний возраст ~35, стандартное отклонение ~15
    age = int(random.normalvariate(35, 15))
    # Ограничиваем разумными пределами
    return max(16, min(85, age))

def _generate_user_activity_profiles(users_count: int, avg_records: int):
    """
    Создает профили активности пользователей.
    80% пользователей - обычная активность
    15% пользователей - высокая активность  
    5% пользователей - низкая активность
    """
    profiles = []
    
    for i in range(users_count):
        activity_type = random.random()
        
        if activity_type < 0.05:  # 5% - низкая активность
            records = int(avg_records * random.uniform(0.1, 0.3))
        elif activity_type < 0.20:  # 15% - высокая активность
            records = int(avg_records * random.uniform(1.5, 3.0))
        else:  # 80% - обычная активность
            records = int(avg_records * random.uniform(0.7, 1.3))
        
        profiles.append(max(1, records))  # Минимум 1 запись
    
    return profiles

def _generate_user_heart_rate_records(user_id: int, records_count: int, start_date: datetime, total_days: int):
    """Генерация записей пульса для конкретного пользователя"""
    
    # Базовый пульс в зависимости от возраста (условно)
    base_heart_rate = random.uniform(65, 85)
    
    records = []
    
    for _ in range(records_count):
        # Случайный день в году
        day_offset = random.randint(0, total_days - 1)
        random_date = start_date + timedelta(days=day_offset)
        
        # Случайное время в течение дня (учитываем активные часы)
        hour = _generate_realistic_hour()
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        timestamp = random_date.replace(
            hour=hour, 
            minute=minute, 
            second=second
        )
        
        # Генерируем реалистичный пульс
        heart_rate = _generate_realistic_heart_rate(base_heart_rate, hour)
        
        records.append({
            "user_id": user_id,
            "timestamp": timestamp,
            "heart_rate": heart_rate
        })
    
    # Сортируем по времени для реалистичности
    records.sort(key=lambda x: x["timestamp"])
    
    return records

def _generate_realistic_hour():
    """Генерация реалистичного часа (больше активности днем)"""
    # Весовое распределение: больше измерений в активные часы
    weights = [
        1, 1, 1, 1, 1, 2,  # 0-5: ночь, редко
        4, 6, 8, 8, 7, 6,  # 6-11: утро, активность растет
        8, 7, 6, 8, 9, 8,  # 12-17: день, пик активности
        7, 6, 4, 3, 2, 1   # 18-23: вечер, спад активности
    ]
    
    return random.choices(range(24), weights=weights)[0]

def _generate_realistic_heart_rate(base_rate: float, hour: int):
    """Генерация реалистичного пульса в зависимости от времени суток"""
    
    # Коэффициенты для разного времени суток
    if 0 <= hour <= 5:  # Ночь - пульс ниже
        multiplier = random.uniform(0.8, 0.9)
    elif 6 <= hour <= 11:  # Утро - умеренный
        multiplier = random.uniform(0.9, 1.1)
    elif 12 <= hour <= 17:  # День - может быть выше (активность)
        multiplier = random.uniform(1.0, 1.3)
    else:  # Вечер - умеренный
        multiplier = random.uniform(0.9, 1.1)
    
    # Добавляем случайную вариацию
    variation = random.uniform(-8, 12)
    heart_rate = base_rate * multiplier + variation
    
    # Ограничиваем разумными пределами
    return max(45, min(180, round(heart_rate)))


# Пример использования
if __name__ == "__main__":
    # Создаем большой набор данных
    load_large_fixtures(
        engine, 
        users_count=10000,
        target_heart_rate_records=2500000
    )
    