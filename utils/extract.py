import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def scrape_page(page_num):
    """
    Mengambil data produk dari 1 halaman website fashion-studio.dicoding.dev.

    Parameters:
    page_num (int): Nomor halaman yang ingin di-scrape (contoh: 1, 2, ..., dst).

    Returns:
    list: Daftar data produk dalam bentuk dictionary, yang masing-masing berisi title, price, rating, colors, size, gender, dan timestamp waktu scraping.

    Raises:
    AttributeError           : Jika elemen HTML produk tidak lengkap saat parsing.
    requests.RequestException: Jika terjadi kesalahan saat melakukan permintaan HTTP.
    """
    if page_num == 1:
        url = 'https://fashion-studio.dicoding.dev/'
    else:
        url = f'https://fashion-studio.dicoding.dev/page{page_num}'

    logging.info(f"Scraping page: {url}")
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"[ERROR] Request failed on page {page_num}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    product_cards = soup.find_all('div', class_='collection-card')

    page_data = []
    timestamp = datetime.now().isoformat()

    for card in product_cards:
        try:
            # Title
            title_tag = card.find(class_='product-title')
            title = title_tag.text.strip() if title_tag else None
            
            # Price (ambil dari dalam .price-container .price atau p.price)
            price = None
            price_container = card.find(class_='price-container')
            if price_container:
                price_tag = price_container.find(class_='price')
                if price_tag:
                    price = price_tag.text.strip()
            if not price:
                price_tag = card.find('p', class_='price')
                if price_tag:
                    price = price_tag.text.strip()
            
            # Inisialisasi field lain dulu
            rating  = None
            colors  = None
            size    = None
            gender  = None
            
            # Cari <p> yang mengandung kata kunci
            for p in card.find_all('p'):
                text = p.text.strip()
                if 'Rating:' in text:
                    rating = text
                elif 'Colors' in text:
                    colors = text
                elif 'Size:' in text:
                    size = text
                elif 'Gender:' in text:
                    gender = text
            
            product = {
                "title": title,
                "price": price,
                "rating": rating,
                "colors": colors,
                "size": size,
                "gender": gender,
                "timestamp": timestamp
            }

            page_data.append(product)

        except AttributeError as e:
            logging.warning(f"[WARNING] Missing field in product on page {page_num}: {e}")
        except Exception as e:
            logging.error(f"[ERROR] Unexpected error on page {page_num}: {e}")
    
    return page_data


def scrape_all_pages(max_pages=50):
    """
    Mengambil data produk dari beberapa halaman secara berurutan dari website fashion-studio.dicoding.dev.

    Parameters:
    max_pages (int): Jumlah maksimum halaman yang akan di-scrape. Default = 50.

    Returns:
    list: Gabungan seluruh data produk dari setiap halaman.
    """
    all_data = []
    for page in range(1, max_pages + 1):
        data = scrape_page(page)
        all_data.extend(data)
    
    logging.info(f"Total products scraped: {len(all_data)}")
    return all_data
