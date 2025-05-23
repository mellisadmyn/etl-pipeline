import pytest
import pandas as pd
from utils.transform import clean_and_transform


def test_valid_data_transformation():
    """Test transformasi data valid ke DataFrame yang bersih."""
    raw_data = [{
        "title": "Cool Jacket",
        "price": "$50.00",
        "rating": "Rating: ⭐ 4.5 / 5",
        "colors": "3 Colors",
        "size": "Size: L",
        "gender": "Gender: Men",
        "timestamp": "2025-05-22T14:00:00"
    }]
    
    df = clean_and_transform(raw_data)

    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 1
    assert df.iloc[0]['price'] == 50.00 * 16000
    assert df.iloc[0]['rating'] == 4.5
    assert df.iloc[0]['colors'] == 3
    assert df.iloc[0]['size'] == "L"
    assert df.iloc[0]['gender'] == "Men"


def test_remove_invalid_title():
    """Baris dengan title 'Unknown Product' harus dibuang."""
    raw_data = [{
        "title": "Unknown Product",
        "price": "$30.00",
        "rating": "Rating: ⭐ 4.0 / 5",
        "colors": "2 Colors",
        "size": "Size: M",
        "gender": "Gender: Women",
        "timestamp": "2025-05-22"
    }]
    
    df = clean_and_transform(raw_data)
    assert df.empty


def test_remove_invalid_rating():
    """Baris dengan rating tidak valid harus dibuang."""
    raw_data = [{
        "title": "T-shirt",
        "price": "$20.00",
        "rating": "Not Rated",
        "colors": "1 Color",
        "size": "Size: S",
        "gender": "Gender: Men",
        "timestamp": "2025-05-22"
    }]
    
    df = clean_and_transform(raw_data)
    assert df.empty


def test_remove_invalid_price():
    """Baris dengan price 'Price Unavailable' harus dibuang."""
    raw_data = [{
        "title": "Jeans",
        "price": "Price Unavailable",
        "rating": "Rating: ⭐ 4.0 / 5",
        "colors": "1 Color",
        "size": "Size: S",
        "gender": "Gender: Men",
        "timestamp": "2025-05-22"
    }]
    
    df = clean_and_transform(raw_data)
    assert df.empty


def test_remove_duplicates():
    """Duplikat baris harus dibuang."""
    raw_data = [{
        "title": "T-shirt",
        "price": "$10.00",
        "rating": "Rating: ⭐ 4.0 / 5",
        "colors": "2 Colors",
        "size": "Size: M",
        "gender": "Gender: Women",
        "timestamp": "2025-05-22"
    }] * 2
    
    df = clean_and_transform(raw_data)
    assert df.shape[0] == 1


def test_input_not_list():
    """Input bukan list harus raise ValueError."""
    with pytest.raises(ValueError, match="Input harus berupa list"):
        clean_and_transform("not a list")


def test_input_empty_list():
    """Input kosong harus raise ValueError."""
    with pytest.raises(ValueError, match="Input harus berupa list"):
        clean_and_transform([])


def test_transformation_error_handling(monkeypatch):
    """Simulasi error internal dan pastikan Exception ditangkap ulang."""
    raw_data = [{
        "title": "Error Test",
        "price": "$10.00",
        "rating": "Rating: ⭐ 5.0 / 5",
        "colors": "2 Colors",
        "size": "Size: M",
        "gender": "Gender: Men",
        "timestamp": "2025-05-22"
    }]

    # Paksa pandas DataFrame untuk raise Exception
    monkeypatch.setattr("utils.transform.pd.DataFrame", lambda x: (_ for _ in ()).throw(Exception("Simulated error")))

    with pytest.raises(Exception, match="Simulated error"):
        clean_and_transform(raw_data)
