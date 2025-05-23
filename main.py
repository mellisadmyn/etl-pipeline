import logging
import pandas as pd
from utils.extract import scrape_all_pages
from utils.transform import clean_and_transform
from utils.load import save_to_csv, save_to_google_sheets, save_to_postgres

# Konfigurasi logging global
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    # Step 1: Extract
    logging.info("Mulai proses scraping data...")
    data = scrape_all_pages()
    df_raw = pd.DataFrame(data)

    if df_raw.empty:
        logging.error("Scraping gagal atau tidak menghasilkan data. Proses dihentikan.")
    else:
        logging.info("Data mentah berhasil diambil dari website fashion-studio.dicoding.dev.")

        # Step 2: Transform
        logging.info("Mulai membersihkan dan mentransformasi data...")
        try:
            df_clean = clean_and_transform(data)
            logging.info("Dataset berhasil dibersihkan dan ditransformasi.")
        except Exception as e:
            logging.error(f"Terjadi kesalahan saat transformasi data: {e}")
        else:
            # Step 3a: Load ke CSV
            clean_path = "products.csv"
            logging.info("Menyimpan data ke file CSV...")
            try:
                save_to_csv(df_clean, clean_path)
                logging.info(f"Data berhasil disimpan ke: {clean_path}")
            except Exception as e:
                logging.error(f"Gagal menyimpan ke CSV: {e}")

            # Step 3b: Load ke Google Sheets
            logging.info("Menyimpan data ke Google Sheets...")
            SPREADSHEET_ID  = '1e_gNgqKhynGGdQGJNsT48YoTroUaQRzWwMRE0AmYo7U'
            RANGE_NAME      = 'Sheet1!A2:J'
            CREDS_PATH      = 'google-sheets-api.json'
            try:
                save_to_google_sheets(df_clean, SPREADSHEET_ID, RANGE_NAME, CREDS_PATH)
                logging.info("Data berhasil disimpan ke Google Sheets.")
            except Exception as e:
                logging.error(f"Gagal menyimpan ke Google Sheets: {e}")

            # Step 3c: Load ke PostgreSQL
            logging.info("Menyimpan data ke database PostgreSQL...")
            try:
                save_to_postgres(
                    df_clean,
                    table_name='products'
                )
                logging.info("Data berhasil disimpan ke database PostgreSQL.")
            except Exception as e:
                logging.error(f"Gagal menyimpan ke PostgreSQL: {e}")
