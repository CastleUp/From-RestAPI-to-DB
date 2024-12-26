import requests
import json
import config

# Конфигурация
BASE_URL = f"{config.base_url}"
HEADERS = {
    f"{config.headers_1}": f"{config.headers_2}"
}

def fetch_all_data():
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

# Скачиваем данные
all_data = fetch_all_data()

# Сохранение в файл (например, JSON)
with open("ref_abp.json", "w", encoding="utf-8") as f:
    json.dump(all_data, f, ensure_ascii=False, indent=4)

print(f"Данные успешно загружены: {len(all_data)} записей сохранено в data.json")