import os
import logging
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from sqlalchemy import create_engine, Table, Column, String, Float, Integer, MetaData, insert
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()


def save_to_csv(df: pd.DataFrame, filename: str):
    """
    Menyimpan DataFrame ke dalam file CSV.

    Parameters:
    df (pd.DataFrame): Data yang akan disimpan.
    filename (str): Nama file .csv yang ingin dibuat.

    Returns:
    None

    Raises:
    Exception: Jika terjadi kegagalan saat menyimpan ke CSV.
    """
    try:
        df.to_csv(filename, index=False)
        logging.info(f"Data berhasil disimpan ke {filename}")
    except Exception as e:
        logging.error("Gagal menyimpan data ke CSV.")
        raise e


def save_to_google_sheets(df: pd.DataFrame, spreadsheet_id: str, range_name: str, creds_json_path: str):
    """
    Menyimpan DataFrame ke Google Sheets menggunakan Google Sheets API.

    Parameters:
    df (pd.DataFrame): Data yang akan disimpan.
    spreadsheet_id (str): ID Google Sheets yang menjadi target penulisan.
    range_name (str): Range lokasi data di Sheets (misal: 'Sheet1!A2').
    creds_json_path (str): Path ke file service account JSON.

    Returns:
    None

    Raises:
    Exception: Jika terjadi kegagalan saat menyimpan ke Google Sheets.
    """
    try:
        # Setup credentials dan Sheets API client
        scopes      = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_file(creds_json_path, scopes=scopes)
        service     = build('sheets', 'v4', credentials=credentials)
        sheet       = service.spreadsheets()

        # Siapkan data dalam format list of lists
        values  = df.values.tolist()
        body    = {
            'values': values
        }

        # Kirim data ke Sheets
        result = sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()

        logging.info(f"Menulis data tanpa header ke range {range_name}, jumlah baris: {len(values)}")
        logging.info("Data berhasil ditulis ke Google Sheets.")
    except Exception as e:
        logging.error(f"Gagal menulis data ke Google Sheets: {e}")
        raise e


def save_to_postgres(df: pd.DataFrame, table_name: str):
    """
    Menyimpan DataFrame ke PostgreSQL menggunakan SQLAlchemy Table dan insert statement.

    Parameters:
    df (pd.DataFrame): Data yang akan disimpan.
    table_name (str): Nama tabel tujuan di database.

    Returns:
    None

    Raises:
    Exception: Jika terjadi kegagalan saat menyimpan ke PostgreSQL.
    """
    try:
        # Ambil kredensial dari .env
        user     = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host     = os.getenv("DB_HOST")
        port     = os.getenv("DB_PORT")
        db       = os.getenv("DB_NAME")

        if not all([user, password, host, port, db]):
            error_msg = "Environment variable DB_USER/DB_PASSWORD/DB_HOST/DB_PORT/DB_NAME belum lengkap!"
            logging.error(error_msg)
            raise EnvironmentError(error_msg)
        else:
            logging.info("Semua environment variable berhasil dimuat.")
            logging.info(f"Database user: {user}")

        # Buat koneksi ke database
        db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(db_url)
        metadata = MetaData()

        # Definisikan struktur tabel
        table = Table(
            table_name,
            metadata,
            Column("title", String),
            Column("price", Float),
            Column("rating", Float),
            Column("colors", Integer),
            Column("size", String),
            Column("gender", String),
            Column("timestamp", String)
        )
        metadata.create_all(engine)

        with engine.connect() as connection:
            logging.info("Terhubung ke PostgreSQL...")

            # Insert data
            data_to_insert = df.to_dict(orient='records')
            logging.info(f"Contoh data yang akan dimasukkan: {data_to_insert[0]}")
            insert_stmt = insert(table).values(data_to_insert)
            connection.execute(insert_stmt)
            connection.commit()

            logging.info("Data berhasil disimpan ke PostgreSQL.")

    except Exception as e:
        logging.error(f"Gagal menyimpan data ke PostgreSQL: {e}")
        raise e
