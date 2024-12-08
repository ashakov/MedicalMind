import os
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import requests
import json

# Функция для управления счетчиком выполнений
def get_execution_count():
    counter_file = 'execution_counter.json'
    try:
        if os.path.exists(counter_file):
            with open(counter_file, 'r') as f:
                data = json.load(f)
                return data.get('count', 0)
        return 0
    except Exception:
        return 0

def increment_execution_count():
    counter_file = 'execution_counter.json'
    try:
        count = get_execution_count() + 1
        with open(counter_file, 'w') as f:
            json.dump({'count': count}, f)
        return count
    except Exception as e:
        print(f"Ошибка при обновлении счетчика: {e}")
        return None

def process_html_file(file_path):
    try:
        # Увеличиваем счетчик выполнений
        execution_number = increment_execution_count()
        print(f"\nНачало обработки файла (Выполнение #{execution_number})")

        # Настройка аутентификации с Google API
        SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        SERVICE_ACCOUNT_FILE = 'D:\\Projects\\MedicalMind\\service_account.json'

        credentials = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        gc = gspread.authorize(credentials)

        # Читаем HTML файл
        with open(file_path, 'r', encoding='windows-1251') as f:
            content = f.read()

        soup = BeautifulSoup(content, 'html.parser')

        # Извлечение информации о клиенте
        client_name = soup.find_all('td', text=lambda x: x and 'Имя:' in x)[0].text.split('Имя: ')[1]
        age = soup.find_all('td', text=lambda x: x and 'Возраст:' in x)[0].text.split('Возраст: ')[1]
        body = soup.find_all('td', text=lambda x: x and 'Телосложение:' in x)[0].text.split('Телосложение: ')[1]
        test_time = soup.find_all('td', text=lambda x: x and 'Время тестирования:' in x)[0].text.split('Время тестирования: ')[1]

        # Извлечение таблиц и их объединение
        tables = []
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            if len(rows) > 0 and len(rows[0].find_all('td')) == 4:
                table_data = []
                for row in rows:
                    cols = [ele.text.strip() for ele in row.find_all('td')]
                    if len(cols) == 4:
                        table_data.append(cols)
                if table_data:
                    df = pd.DataFrame(table_data)
                    tables.append(df)

        # Объединение всех таблиц в один DataFrame
        combined_df = pd.concat(tables, ignore_index=True)

        # Добавление информации о клиенте в DataFrame
        combined_df['Client_Name'] = client_name
        combined_df['Возраст'] = age
        combined_df['Телосложение'] = body
        combined_df['Время тестирования'] = test_time

        # Установка первой строки в качестве заголовков столбцов
        combined_df.columns = combined_df.iloc[0]

        # Сброс индекса и переименование столбцов
        result_df = combined_df.reset_index(drop=True)
        result_df.columns = ['Измеряемый параметр', 'Диапазон нормальных значений', 'Результат',
                           'Интерпретация результата', 'ФИО клиента', 'Возраст', 'Телосложение', 'Время тестирования']
        result_df = result_df[1:]

        # Доступ к Google Sheets
        spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1-ox9vCOf59cmfcjB0agpHsj-Tn2P9jNpIIoIXxnQSlQ'
        spreadsheet = gc.open_by_url(spreadsheet_url)

        # Выбираем лист "Вставка"
        worksheet = spreadsheet.worksheet('Вставка')

        # Записываем DataFrame в Google Sheets
        set_with_dataframe(worksheet, result_df)

        # Определение формата названия файла
        client_last_name, client_first_name = client_name.split()[:2]
        order_number = 1
        pdf_filename = f"Отчет_{client_last_name}_{client_first_name}_{order_number}.pdf"

        # Экспорт конкретной вкладки в PDF
        export_sheet_to_pdf(spreadsheet.id, '0', pdf_filename, credentials)

        print(f"Все операции успешно выполнены! (Выполнение #{execution_number})")
        return True

    except Exception as e:
        print(f"Произошла ошибка при обработке отчета: {e}")
        return False

def export_sheet_to_pdf(spreadsheet_id, gid, save_path, credentials):
    try:
        # URL для экспорта конкретной вкладки
        export_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=pdf&gid={gid}"

        headers = {
            'Authorization': 'Bearer ' + credentials.token,
        }

        response = requests.get(export_url, headers=headers)

        with open(save_path, 'wb') as f:
            f.write(response.content)
    except Exception as e:
        print(f"Ошибка при экспорте в PDF: {e}")

if __name__ == "__main__":
    print("Программа обработки медицинских отчетов")
    print("----------------------------------------")
    
    while True:
        file_path = input("\nВведите путь к HTML файлу (или 'q' для выхода): ")
        
        if file_path.lower() == 'q':
            break
            
        if not os.path.exists(file_path):
            print("Файл не найден. Пожалуйста, проверьте путь к файлу.")
            continue
            
        if not file_path.endswith(('.html', '.htm')):
            print("Файл должен быть в формате HTML (.html или .htm)")
            continue
            
        process_html_file(file_path)
