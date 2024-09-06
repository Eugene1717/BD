from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Подключение к MongoDB
try:
    # Подключаемся к локальному серверу MongoDB
    client = MongoClient("mongodb://localhost:65000/")
    print("Connected successfully to MongoDB")
except ConnectionFailure as e:
    print(f"Could not connect to MongoDB: {e}")

# Выбираем базу данных и коллекцию
db = client["test_database"]
collection = db["test_collection"]

# Вставка документов
document_1 = {
    "name": "Alice",
    "age": 25,
    "email": "alice@example.com",
    "skills": ["Python", "MongoDB", "Data Analysis"]
}

document_2 = {
    "name": "Bob",
    "age": 30,
    "email": "bob@example.com",
    "skills": ["Java", "SQL", "Spring"]
}

# Вставка одного документа
insert_result_1 = collection.insert_one(document_1)
print(f"Inserted document id: {insert_result_1.inserted_id}")

# Вставка нескольких документов
insert_result_2 = collection.insert_many([document_1, document_2])
print(f"Inserted document ids: {insert_result_2.inserted_ids}")

# Выборка всех документов
print("\nAll documents in the collection:")
documents = collection.find()
for doc in documents:
    print(doc)

# Фильтрация данных по возрасту (больше 25 лет)
print("\nDocuments where age > 25:")
filter_query = {"age": {"$gt": 25}}
filtered_documents = collection.find(filter_query)
for doc in filtered_documents:
    print(doc)

# Получение одного документа (по имени)
print("\nFind one document where name is 'Alice':")
alice_doc = collection.find_one({"name": "Alice"})
print(alice_doc)

# Обновление одного документа (возраст Alice -> 26)
update_result = collection.update_one({"name": "Alice"}, {"$set": {"age": 26}})
print(f"\nDocuments updated: {update_result.modified_count}")

# Обновление нескольких документов (установка статуса 'senior' для тех, кто старше 25 лет)
update_result = collection.update_many({"age": {"$gt": 25}}, {"$set": {"status": "senior"}})
print(f"Documents updated: {update_result.modified_count}")

# Удаление одного документа (по имени)
delete_result = collection.delete_one({"name": "Alice"})
print(f"\nDocuments deleted (name='Alice'): {delete_result.deleted_count}")

# Удаление нескольких документов (по возрасту больше 25)
delete_result = collection.delete_many({"age": {"$gt": 25}})
print(f"Documents deleted (age > 25): {delete_result.deleted_count}")

# Создание индекса по полю 'email' для ускорения поиска
print("\nCreating index on 'email' field:")
index_name = collection.create_index("email")
print(f"Created index: {index_name}")

# Пример использования транзакций (требуется Replica Set)
print("\nTransaction example:")
session = client.start_session()
try:
    with session.start_transaction():
        collection.insert_one({"name": "Charlie", "age": 35}, session=session)
        collection.insert_one({"name": "David", "age": 40}, session=session)
    print("Transaction committed successfully")
except Exception as e:
    print(f"Transaction failed: {e}")
finally:
    session.end_session()

# Закрытие подключения к базе данных
client.close()
