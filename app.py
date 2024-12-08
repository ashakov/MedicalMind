import os
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import requests
import io
from IPython.display import display, clear_output
import ipywidgets as widgets
import json


# Настройка аутентификации с Google API
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'D:\Projects\MedicalMind\service_account.json'  # Укажите путь к вашему файлу сервисного аккаунта

credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(credentials)


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

def process_report(uploaded_file):
    try:
        # Увеличиваем счетчик выполнений
        execution_number = increment_execution_count()
        
        with output:
            clear_output()
            print(f"Начало обработки файла (Выполнение #{execution_number})")
        
        content = uploaded_file['content']
        soup = BeautifulSoup(content.decode('windows-1251'), 'html.parser')

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
        # Получаем фамилию и имя клиента
        client_last_name, client_first_name = client_name.split()[:2]
        order_number = 1  # Здесь можно реализовать логику увеличения номера
        pdf_filename = f"Отчет_{client_last_name}_{client_first_name}_{order_number}.pdf"

        # Экспорт конкретной вкладки в PDF
        export_sheet_to_pdf(spreadsheet.id, '0', pdf_filename)

        # Загрузка PDF на Google Диск
        drive_folder_id = 'ваш_id_папки_на_диске'
        upload_to_google_drive(pdf_filename, drive_folder_id, credentials)

        # Отображение сообщения об успехе
        with output:
            clear_output()
            print(f"Все операции успешно выполнены! (Выполнение #{execution_number})")

    except Exception as e:
        with output:
            clear_output()
            print(f"Произошла ошибка при обработке отчета: {e}")

def export_sheet_to_pdf(spreadsheet_id, gid, save_path):
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

def upload_to_google_drive(file_path, drive_folder_id, credentials):
    try:
        drive_service = build('drive', 'v3', credentials=credentials)
        file_metadata = {'name': os.path.basename(file_path)}
        if drive_folder_id:
            file_metadata['parents'] = [drive_folder_id]

        media = MediaFileUpload(file_path, mimetype='application/pdf')
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"Файл загружен на Google Диск с ID: {file.get('id')}")
    except Exception as e:
        print(f"Ошибка при загрузке на Google Диск: {e}")

# Создаем виджет для загрузки файла
upload_button = widgets.FileUpload(
    accept='.html, .htm',
    multiple=False
)

# Кнопка для запуска обработки
process_button = widgets.Button(
    description='Обработать отчет',
    button_style='success',
    tooltip='Нажмите, чтобы обработать отчет',
    icon='check'
)

# Вывод для отображения сообщений
output = widgets.Output()

# Функция, вызываемая при нажатии на кнопку
def on_process_button_clicked(b):
    if upload_button.value:
        uploaded_file = next(iter(upload_button.value.values()))
        process_report(uploaded_file)
    else:
        with output:
            clear_output()
            print("Пожалуйста, загрузите файл отчета.")

# Привязываем функцию к кнопке
process_button.on_click(on_process_button_clicked)

# Отображаем виджеты
display(upload_button, process_button, output)
