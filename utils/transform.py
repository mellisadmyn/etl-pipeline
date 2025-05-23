import pandas as pd
import logging
import re

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def clean_and_transform(raw_data):
    """
    Membersihkan dan mentransformasi data hasil scraping menjadi dataset yang bersih dan terstruktur.

    Parameters:
    raw_data (list): List of dictionary berisi data mentah hasil scraping.

    Returns:
    pd.DataFrame: DataFrame yang sudah dibersihkan dan ditransformasi.

    Raises:
    ValueError: Jika data tidak berbentuk list atau kosong.
    Exception: Jika terjadi kesalahan saat proses pembersihan atau transformasi data.
    """
    if not isinstance(raw_data, list) or not raw_data:
        raise ValueError("Input harus berupa list dan tidak boleh kosong.")

    try:
        df = pd.DataFrame(raw_data)

        # Buang baris dengan nilai null
        df.dropna(inplace=True)

        # Buang baris dengan data invalid eksplisit
        df = df[~df['title'].str.lower().str.contains("unknown product")]
        df = df[~df['rating'].isin(["Invalid Rating / 5", "Not Rated"])]
        df = df[~df['price'].isin(["Price Unavailable", None])]

        # Buang duplikat
        df.drop_duplicates(inplace=True)

        # Membersihkan dan konversi kolom `price`
        df['price'] = df['price'].str.replace(r'[^0-9.]', '', regex=True)
        df['price'] = df['price'].astype(float) * 16000

        # Membersihkan dan konversi kolom `rating`
        df['rating'] = df['rating'].str.extract(r'(\d+\.?\d*)')
        df['rating'] = df['rating'].astype(float)

        # Membersihkan dan konversi kolom `colors`
        df['colors'] = df['colors'].str.extract(r'(\d+)').astype(int)

        # Membersihkan kolom `size` dan `gender`
        df['size'] = df['size'].str.replace("Size: ", "").str.strip()
        df['gender'] = df['gender'].str.replace("Gender: ", "").str.strip()

        # Validasi tipe data kolom
        expected_dtypes = {
            'title' : 'object',
            'price' : 'float64',
            'rating': 'float64',
            'colors': 'int64',
            'size'  : 'object',
            'gender': 'object'
        }

        for column, expected_type in expected_dtypes.items():
            actual_type = df[column].dtype
            if str(actual_type) == expected_type:
                logging.info(f"Tipe data kolom '{column}' sudah sesuai: {actual_type}")
            else:
                logging.warning(f"Tipe data kolom '{column}' TIDAK SESUAI! Diharapkan: {expected_type}, Aktual: {actual_type}")

        # Reset index
        df.reset_index(drop=True, inplace=True)

        logging.info(f"Data berhasil dibersihkan: {df.shape[0]} baris")

        return df

    except Exception as e:
        logging.error(f"Error saat membersihkan dan mentransformasi data: {e}")
        raise e
