import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import requests
from google.auth.transport.requests import Request
from datetime import datetime

# =======================
# Настройка страницы
# =======================

# Вызов set_page_config() как первой Streamlit-команды
st.set_page_config(
    page_title="Загрузчик HTML в Google Sheets и Drive",
    layout="centered",
    initial_sidebar_state="expanded",
)

# =======================
# Добавление Логотипа
# =======================

# Путь к файлу логотипа
logo_path = "logo.png"  # Убедитесь, что файл logo.png находится в той же директории

# Проверка наличия файла логотипа
if os.path.exists(logo_path):
    st.image(logo_path, width=150)  # Настройте ширину по необходимости
else:
    st.warning("Логотип не найден. Убедитесь, что файл `logo.png` находится в директории приложения.")

# =======================
# Инициализация Сервисов
# =======================

def initialize_services():
    """
    Инициализирует сервисы Google Sheets и Google Drive используя Streamlit Secrets.
    """
    gcp_config = st.secrets["gcp_service_account"]
    google_api_config = st.secrets["google_api"]
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]  # Жестко закодированные scopes

    # Сборка словаря с учетными данными сервисного аккаунта
    service_account_info = {
        "type": gcp_config["type"],
        "project_id": gcp_config["project_id"],
        "private_key_id": gcp_config["private_key_id"],
        "private_key": gcp_config["private_key"],
        "client_email": gcp_config["client_email"],
        "client_id": gcp_config["client_id"],
        "auth_uri": gcp_config["auth_uri"],
        "token_uri": gcp_config["token_uri"],
        "auth_provider_x509_cert_url": gcp_config["auth_provider_x509_cert_url"],
        "client_x509_cert_url": gcp_config["client_x509_cert_url"],
        "universe_domain": gcp_config["universe_domain"]
    }

    try:
        # Создание учетных данных
        creds = Credentials.from_service_account_info(
            service_account_info,
            scopes=scopes
        )
    except Exception as e:
        raise ValueError(f"Не удалось создать учетные данные: {e}")

    try:
        # Инициализация gspread клиента
        gc = gspread.authorize(creds)
    except Exception as e:
        raise ConnectionError(f"Не удалось авторизоваться в gspread: {e}")

    try:
        # Инициализация Google Sheets API
        sheets_service = build('sheets', 'v4', credentials=creds)
    except Exception as e:
        raise ConnectionError(f"Не удалось инициализировать Google Sheets API: {e}")

    try:
        # Инициализация Google Drive API
        drive_service = build('drive', 'v3', credentials=creds)
    except Exception as e:
        raise ConnectionError(f"Не удалось инициализировать Google Drive API: {e}")

    return creds, gc, sheets_service, drive_service

# Попытка инициализировать сервисы
try:
    creds, gc, sheets_service, drive_service = initialize_services()
except Exception as e:
    st.error(f"Ошибка при загрузке конфигурации или инициализации сервисов: {e}")
    st.stop()

# =======================
# Streamlit App Layout
# =======================

# Заголовок приложения
st.title("Загрузка HTML Отчёта в Google Sheets и Drive")

# Инструкции
st.markdown("""
    Загрузите HTML отчёт, и это приложение извлечёт необходимые данные и добавит их в ваш документ Google Sheets.
    После этого, приложение экспортирует определённый лист в PDF и загрузит его на ваш Google Диск.
""")

# Загрузка файла
uploaded_file = st.file_uploader("Выберите HTML файл", type=["html", "htm"])

if uploaded_file is not None:
    try:
        # Чтение загруженного файла
        content = uploaded_file.read().decode('windows-1251')  # При необходимости измените кодировку
        soup = BeautifulSoup(content, 'html.parser')

        # Извлечение информации о клиенте
        client_name_element = soup.find('td', text=lambda x: x and 'Имя:' in x)
        if client_name_element:
            client_name = client_name_element.text.split('Имя: ')[1].strip()
        else:
            raise ValueError("Не удалось найти поле 'Имя' в HTML файле.")

        age_element = soup.find('td', text=lambda x: x and 'Возраст:' in x)
        if age_element:
            age = age_element.text.split('Возраст: ')[1].strip()
        else:
            raise ValueError("Не удалось найти поле 'Возраст' в HTML файле.")

        body_element = soup.find('td', text=lambda x: x and 'Телосложение:' in x)
        if body_element:
            body = body_element.text.split('Телосложение: ')[1].strip()
        else:
            raise ValueError("Не удалось найти поле 'Телосложение' в HTML файле.")

        test_time_element = soup.find('td', text=lambda x: x and 'Время тестирования:' in x)
        if test_time_element:
            test_time = test_time_element.text.split('Время тестирования: ')[1].strip()
        else:
            raise ValueError("Не удалось найти поле 'Время тестирования' в HTML файле.")

        # Извлечение таблиц с 4 столбцами
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

        if not tables:
            st.error("В загруженном HTML файле не найдено подходящих таблиц.")
            st.stop()

        # Объединение всех таблиц в один DataFrame
        combined_df = pd.concat(tables, ignore_index=True)

        # Добавление информации о клиенте
        combined_df['Client_Name'] = client_name
        combined_df['Возраст'] = age
        combined_df['Телосложение'] = body
        combined_df['Время тестирования'] = test_time

        # Установка первой строки в качестве заголовков
        combined_df.columns = combined_df.iloc[0]
        result_df = combined_df.reset_index(drop=True)
        result_df.columns = [
            'Измеряемый параметр',
            'Диапазон нормальных значений',
            'Результат',
            'Интерпретация результата',
            'ФИО клиента',
            'Возраст',
            'Телосложение',
            'Время тестирования'
        ]
        result_df = result_df[1:]

        # Отображение DataFrame
        st.subheader("Извлечённые Данные")
        st.dataframe(result_df)

        # Получение значений из Streamlit Secrets
        spreadsheet_url = st.secrets["google_api"]["SPREADSHEET_URL"]
        worksheet_name = "Вставка"  # Жестко заданное имя листа
        drive_folder_id = st.secrets["google_api"]["DRIVE_FOLDER_ID"]

        # Кнопка для загрузки данных
        if st.button("Загрузить в Google Sheets и Drive"):
            try:
                if not spreadsheet_url:
                    raise ValueError("URL Google Sheets не указан в конфигурации.")

                # Открытие таблицы по URL
                spreadsheet = gc.open_by_url(spreadsheet_url)

                # Выбор листа
                try:
                    worksheet = spreadsheet.worksheet(worksheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    # Если лист не существует, создаём новый
                    worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")

                # Запись DataFrame в Google Sheets
                set_with_dataframe(worksheet, result_df)

                st.success("Данные успешно записаны в Google Sheets!")

                # =======================
                # Генерация PDF
                # =======================

                # Получение идентификатора таблицы и листа
                spreadsheet_id = spreadsheet.url.split('/d/')[1].split('/')[0]
                sheet = spreadsheet.worksheet(worksheet_name)
                sheet_id = sheet.id  # Это gid

                # Формирование названия файла
                try:
                    client_last_name, client_first_name = client_name.split()[:2]
                except ValueError:
                    client_last_name = client_name
                    client_first_name = "Unknown"

                # Генерация уникального номера (временная метка)
                order_number = datetime.now().strftime("%Y%m%d%H%M%S")

                pdf_filename = f"Отчет_{client_last_name}_{client_first_name}_{order_number}.pdf"

                # Формирование URL для экспорта PDF
                export_url = (
                    f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?"
                    f"format=pdf&"
                    f"gid={sheet_id}&"
                    f"size=letter&"
                    f"portrait=true&"
                    f"fitw=true&"
                    f"sheetnames=false&"
                    f"printtitle=false&"
                    f"pagenumbers=false&"
                    f"gridlines=false&"
                    f"fzr=false"
                )

                # Обновление токена, если необходимо
                if not creds.valid:
                    creds.refresh(Request())

                headers = {
                    'Authorization': f'Bearer {creds.token}',
                }

                # Скачиваем PDF
                response = requests.get(export_url, headers=headers)
                if response.status_code != 200:
                    st.error("Не удалось экспортировать лист в PDF.")
                    st.stop()

                # Сохранение PDF файла
                with open(pdf_filename, 'wb') as f:
                    f.write(response.content)

                st.success(f"Лист успешно экспортирован в PDF: {pdf_filename}")

                # =======================
                # Загрузка PDF на Google Drive
                # =======================

                def upload_to_google_drive(file_path, folder_id=None):
                    file_metadata = {'name': os.path.basename(file_path)}
                    if folder_id:
                        file_metadata['parents'] = [folder_id]

                    media = MediaFileUpload(file_path, mimetype='application/pdf')
                    file = drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id'
                    ).execute()

                    return file.get('id')

                # Загрузка файла
                if drive_folder_id.strip() == "":
                    drive_folder_id = None  # Если поле пустое, загружаем в корень

                uploaded_file_id = upload_to_google_drive(pdf_filename, drive_folder_id)

                st.success(f"PDF успешно загружен на Google Диск с ID: {uploaded_file_id}")

                # Удаление локального PDF файла (опционально)
                os.remove(pdf_filename)

            except Exception as e:
                st.error(f"Произошла ошибка: {e}")

    except Exception as e:
        st.error(f"Не удалось обработать загруженный файл: {e}")
