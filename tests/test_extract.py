import pytest
import requests
import requests_mock
from utils.extract import scrape_page, scrape_all_pages
import logging


@pytest.fixture
def mock_product_html():
    """Mock HTML untuk product page."""
    return """
    <div class="collection-card">
        <div style="position: relative;">
            <img src="https://picsum.photos/280/350?random=1" class="collection-image" alt="T-shirt Uniqlo 1">
        </div>
        <div class="product-details">
            <h3 class="product-title">Unit Test Product Cap</h3>
            <div class="price-container"><span class="price">$55.00</span></div>
            <p>Rating: ⭐ 4.2 / 5</p>
            <p>3 Colors</p>
            <p>Size: M, L</p>
            <p>Gender: Women</p>
        </div>
    </div>
    """

@pytest.fixture
def mock_fallback_price_html():
    """Mock HTML untuk suatu product page dengan fallback price."""
    return """
    <div class="collection-card">
        <h3 class="product-title">Fallback Product</h3>
        <p class="price">Rp 99.000</p>
        <p>Rating: ⭐ 4.5 / 5</p>
        <p>Colors: 2 Colors</p>
        <p>Size: S</p>
        <p>Gender: Men</p>
    </div>
    """


def test_scrape_page_http_error(caplog):
    """Test scrape_page meng-handle HTTP error (404)."""
    with requests_mock.Mocker() as m:
        m.get("https://fashion-studio.dicoding.dev/", status_code=404)

        with caplog.at_level(logging.ERROR):
            results = scrape_page(1)

        assert results == []
        assert "Request failed on page 1" in caplog.text


def test_scrape_page_success(mock_product_html):
    """Test scraping halaman berhasil dengan 2 produk."""
    with requests_mock.Mocker() as m:
        m.get("https://fashion-studio.dicoding.dev/",
              text=f"<html><body>{mock_product_html * 2}</body></html>")

        results = scrape_page(1)

        assert isinstance(results, list)
        assert len(results) == 2

        product = results[0]
        assert product['title'] == "Unit Test Product Cap"
        assert product['price'] == "$55.00"
        assert "4.2" in product['rating']
        assert "3 Colors" in product['colors']
        assert product['size'] == "Size: M, L"
        assert product['gender'] == "Gender: Women"
        assert isinstance(product['timestamp'], str)


def test_scrape_page_timeout(caplog):
    """Test scrape_page meng-handle timeout."""
    with requests_mock.Mocker() as m:
        m.get("https://fashion-studio.dicoding.dev/", exc=requests.exceptions.ConnectTimeout)

        with caplog.at_level(logging.ERROR):
            results = scrape_page(1)

        assert results == []
        assert "Request failed on page 1" in caplog.text


def test_scrape_all_pages(monkeypatch):
    """Test scrape_all_pages memanggil scrape_page berkali-kali dan menggabungkan hasil."""
    def dummy_scrape_page(page_num):
        return [{"title": f"Dummy Product {page_num}"}]

    monkeypatch.setattr("utils.extract.scrape_page", dummy_scrape_page)

    results = scrape_all_pages(max_pages=3)

    assert isinstance(results, list)
    assert len(results) == 3
    assert results[0]['title'] == "Dummy Product 1"
    assert results[2]['title'] == "Dummy Product 3"


def test_scrape_page_price_fallback(mock_fallback_price_html):
    """Test scrape_page menggunakan fallback <p class='price'> ketika price-container tidak ada."""
    with requests_mock.Mocker() as m:
        m.get("https://fashion-studio.dicoding.dev/", text=f"<html><body>{mock_fallback_price_html}</body></html>")
        results = scrape_page(1)

    assert len(results) == 1
    assert results[0]["price"] == "Rp 99.000"
    assert results[0]["title"] == "Fallback Product"

