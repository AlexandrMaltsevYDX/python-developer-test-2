# Задача на позицию Python Developer №2

## Контекст
У вас есть код, который использует sqlalchemy-core для запросов в базу данных, вам нужно максимально возможно улучшить этот код по вашему усмотрению и заменить комментарии под названием функций на соответствующие запросы. Учитывайте, что в таблицах могут быть десятки и сотни миллионов строк, количество запросов на чтение таблиц могут измеряться десятками в секунду.

## Что надо сделать
* Улучшить текущий код, представленный в разделе [Пример кода] сделав его более эффективным, читаемым и безопасным
* Заменить комментарии в функциях на соответствующие SQL запросы.

## Пример кода
``` python
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey

engine = create_engine('postgresql://username:password@host:port/database_name')
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

def query_users(min_age: int, gender: str, min_avg_heart_rate: float, date_from: datetime, date_to: datetime):
    # Напишите здесь запрос, который возвращает всех пользователей, которые старше'min_age' и 
    # имеют средний пульс выше, чем 'min_avg_heart_rate', на определенном промежутке времени
        # min_age: минимальный возраст пользователей
        # gender: пол пользователей
        # min_avg_heart_rate: минимальный средний пульс
        # date_from: начало временного промежутка
        # date_to: конец временного промежутка
    return
    
def query_top(user_id: int, date_from: datetime, date_to: datetime):
    # Напишите здесь запрос, который возвращает топ 10 самых высоких средних показателей 'heart_rate' 
    # за часовые промежутки в указанном периоде 'date_from' и 'date_to'
        # user_id: ID пользователя
        # date_from: начало временного промежутка
        # date_to: конец временного промежутка
    return
```
## Ожидаемый результат
* Подготовьте ссылку на файл с кодом в удобном для вас формате, где будет ясно видно выполнение всех требований ТЗ и правильная работа кода.
* После завершения вставьте ссылку на решение задачи в текстовое поле анкеты.