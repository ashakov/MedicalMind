# app.py
import os
import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import requests

from google.auth.transport.requests import Request
import traceback

# Настройка аутентификации с Google API
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'service_account.json'  # Укажите правильный путь

@st.cache_resource
def get_google_credentials():
    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    credentials.refresh(Request())  # Обновление токена доступа
    return credentials

def get_gspread_client(credentials):
    gc = gspread.authorize(credentials)
    return gc

def export_sheet_to_pdf(spreadsheet_id, gid, save_path, credentials):
    try:
        # URL для экспорта конкретной вкладки
        export_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=pdf&gid={gid}"

        headers = {
            'Authorization': 'Bearer ' + credentials.token,
        }

        response = requests.get(export_url, headers=headers)

        if response.status_code != 200:
            raise ValueError(f"Не удалось экспортировать лист в PDF. Статус код: {response.status_code}")

        with open(save_path, 'wb') as f:
            f.write(response.content)
        st.success("Вкладка экспортирована в PDF.")
    except Exception as e:
        st.error(f"Ошибка при экспорте в PDF: {e}")
        st.text(traceback.format_exc())

def upload_to_google_drive(file_path, drive_folder_id, credentials):
    try:
        drive_service = build('drive', 'v3', credentials=credentials)
        file_metadata = {'name': os.path.basename(file_path)}
        if drive_folder_id:
            file_metadata['parents'] = [drive_folder_id]

        media = MediaFileUpload(file_path, mimetype='application/pdf')
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        st.success(f"Файл загружен на Google Диск с ID: {file.get('id')}")
    except Exception as e:
        st.error(f"Ошибка при загрузке на Google Диск: {e}")
        st.text(traceback.format_exc())

def extract_text(soup, label):
    try:
        element = soup.find_all('td', string=lambda x: x and label in x)[0]
        extracted_text = element.text.split(f'{label} ')[1]
        st.info(f"{label} извлечено: {extracted_text}")
        return extracted_text
    except (IndexError, AttributeError) as e:
        st.error(f"Не удалось найти или обработать {label}: {e}")
        st.text(traceback.format_exc())
        return None

def process_report(uploaded_file):
    try:
        st.info("Начало обработки файла.")

        content = uploaded_file.read()
        st.info("Файл прочитан.")

        soup = BeautifulSoup(content.decode('windows-1251'), 'html.parser')
        st.info("HTML-файл распарсен.")

        # Извлечение информации о клиенте
        client_name = extract_text(soup, 'Имя:')
        age = extract_text(soup, 'Возраст:')
        body = extract_text(soup, 'Телосложение:')
        test_time = extract_text(soup, 'Время тестирования:')

        if not all([client_name, age, body, test_time]):
            st.error("Не удалось извлечь все необходимые данные.")
            return

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

        st.info(f"Найдено таблиц: {len(tables)}")

        if not tables:
            st.error("Не найдены подходящие таблицы в отчёте.")
            return

        # Объединение всех таблиц в один DataFrame
        combined_df = pd.concat(tables, ignore_index=True)
        st.info("Таблицы объединены в один DataFrame.")

        # Добавление информации о клиенте в DataFrame
        combined_df['Client_Name'] = client_name
        combined_df['Возраст'] = age
        combined_df['Телосложение'] = body
        combined_df['Время тестирования'] = test_time
        st.info("Информация о клиенте добавлена в DataFrame.")

        # Установка первой строки в качестве заголовков столбцов
        combined_df.columns = combined_df.iloc[0]
        st.info("Установлены заголовки столбцов.")

        # Сброс индекса и переименование столбцов
        result_df = combined_df.reset_index(drop=True)
        result_df.columns = ['Измеряемый параметр', 'Диапазон нормальных значений', 'Результат',
                             'Интерпретация результата', 'ФИО клиента', 'Возраст', 'Телосложение', 'Время тестирования']
        result_df = result_df[1:]
        st.info("Переименованы столбцы и удалена первая строка.")

        # Доступ к Google Sheets
        spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1-ox9vCOf59cmfcjB0agpHsj-Tn2P9jNpIIoIXxnQSlQ'
        credentials = get_google_credentials()
        gc = get_gspread_client(credentials)
        spreadsheet = gc.open_by_url(spreadsheet_url)
        st.info("Подключение к Google Sheets выполнено.")

        # Выбираем лист "Вставка"
        worksheet = spreadsheet.worksheet('Вставка')
        st.info("Выбран лист 'Вставка'.")

        # Записываем DataFrame в Google Sheets
        set_with_dataframe(worksheet, result_df)
        st.success("Данные записаны в Google Sheets.")

        # Определение формата названия файла
        client_last_name, client_first_name = client_name.split()[:2]
        order_number = 1  # Здесь можно реализовать логику увеличения номера
        pdf_filename = f"Отчет_{client_last_name}_{client_first_name}_{order_number}.pdf"
        st.info(f"Название PDF файла: {pdf_filename}")

        # Экспорт конкретной вкладки в PDF
        export_sheet_to_pdf(spreadsheet.id, '0', pdf_filename, credentials)

        # Загрузка PDF на Google Диск
        drive_folder_id = '1tYxHdnlvfsuNEsGrBg0wEnmBEP7RTHpG'  # Замените на реальный ID папки
        upload_to_google_drive(pdf_filename, drive_folder_id, credentials)

        st.success("Все операции успешно выполнены!")

    except Exception as e:
        st.error(f"Произошла ошибка при обработке отчета: {e}")
        st.text(traceback.format_exc())

def main():
    st.title("Обработка Медицинских Отчётов")

    st.write("""
    Загрузите ваш HTML-файл отчёта, и приложение обработает его, извлечёт данные и загрузит результаты в Google Sheets и Google Drive.
    """)

    uploaded_file = st.file_uploader("Загрузите HTML файл отчёта", type=["html", "htm"])

    if uploaded_file is not None:
        if st.button("Обработать отчет"):
            process_report(uploaded_file)

if __name__ == "__main__":
    main()
