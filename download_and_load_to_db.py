import requests
import psycopg2
import config

# Конфигурация
BASE_URL = f"{config.base_url}"
HEADERS = {
    f"{config.headers_1}": f"{config.headers_2}"
}

def fetch_all_data():
    """Скачиваем все данные с API"""
    all_items = []
    next_page = BASE_URL  # Начальная страница

    while next_page:
        response = requests.get(next_page, headers=HEADERS)
        
        # Проверка успешности запроса
        if response.status_code != 200:
            print(f"Ошибка при запросе: {response.status_code}, {response.text}")
            break
        
        # Парсинг данных
        data = response.json()
        all_items.extend(data.get("items", []))
        
        # Получение следующей страницы
        next_page = data.get("next_page")
        if next_page:
            next_page = "https://ows.goszakup.gov.kz" + next_page
    
    return all_items

def save_to_database(data):
    """Сохраняем данные в базу данных"""
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**config.DB_CONFIG)
        cur = conn.cursor()

        # Создаем таблицу, если её нет
        create_table_query = """
        CREATE TABLE IF NOT EXISTS gz.ref_fkrb_subprogram (
            id SERIAL PRIMARY KEY,
            ppr VARCHAR(50),
            name_ru TEXT,
            name_kz TEXT
            );
        """
        cur.execute(create_table_query)

        # Вставляем данные
        insert_query = """
        INSERT INTO gz.ref_fkrb_subprogram (id, ppr, name_ru, name_kz)
        VALUES (%s, %s, %s, %s);
        """
        for item in data:
            cur.execute(insert_query, (
                item.get("id", None),
                item.get("ppr", None),
                item.get("name_ru", None),
                item.get("name_kz", None)
            ))
        
        # Сохраняем изменения
        conn.commit()
        print(f"{len(data)} записей успешно добавлено в базу данных.")

    except Exception as e:
        print(f"Ошибка при сохранении данных в базу: {e}")
    finally:
        # Закрываем соединение
        cur.close()
        conn.close()

# Скачиваем данные из API
all_data = fetch_all_data()

# Сохраняем в базу данных
if all_data:
    save_to_database(all_data)
