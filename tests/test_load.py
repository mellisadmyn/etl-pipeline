import pytest
import pandas as pd
from utils.load import save_to_csv, save_to_google_sheets, save_to_postgres
from unittest.mock import patch, MagicMock


@pytest.fixture
def sample_df():
    """Fixture sample DataFrame untuk testing."""
    return pd.DataFrame([{
        "title": "Product A", "price": 7802560.0, "rating": 4.5, "colors": 2,
        "size": "M", "gender": "Men", "timestamp": "2025-05-22T10:00:00"
    }])


def test_save_to_csv_success(tmp_path, sample_df):
    """Test penyimpanan DataFrame ke CSV berhasil."""
    file_path = tmp_path / "test_output.csv"
    save_to_csv(sample_df, str(file_path))
    assert file_path.exists()


def test_save_to_csv_failure(sample_df):
    """Test penyimpanan DataFrame ke CSV gagal ketika terjadi exception."""
    with patch.object(sample_df, "to_csv", side_effect=Exception("Write failed")):
        with pytest.raises(Exception, match="Write failed"):
            save_to_csv(sample_df, "dummy.csv")


@patch("utils.load.Credentials")
@patch("utils.load.build")
def test_save_to_google_sheets_success(mock_build, mock_creds, sample_df):
    """Test penyimpanan DataFrame ke Google Sheets berhasil menggunakan mock."""
    mock_service = MagicMock()
    mock_values = MagicMock()
    
    mock_build.return_value.spreadsheets.return_value = mock_service
    mock_service.values.return_value = mock_values
    mock_values.update.return_value.execute.return_value = {}

    save_to_google_sheets(sample_df, "spreadsheet_id", "Sheet1!A2", "fake_creds.json")

    mock_values.update.assert_called_once_with(
        spreadsheetId="spreadsheet_id",
        range="Sheet1!A2",
        valueInputOption="RAW",
        body={'values': sample_df.values.tolist()}
    )



@patch("utils.load.Credentials")
@patch("utils.load.build")
def test_save_to_google_sheets_failure(mock_build, mock_creds, sample_df):
    """Test penyimpanan DataFrame ke Google Sheets gagal ketika terjadi exception."""
    mock_service = MagicMock()
    mock_service.values().update().execute.side_effect = Exception("Google API error")
    mock_build.return_value.spreadsheets.return_value = mock_service

    with pytest.raises(Exception, match="Google API error"):
        save_to_google_sheets(sample_df, "spreadsheet_id", "Sheet1!A2", "fake_creds.json")


@patch("utils.load.create_engine")
@patch("utils.load.os.getenv")
def test_save_to_postgres_success(mock_getenv, mock_engine, sample_df):
    """Test penyimpanan DataFrame ke PostgreSQL berhasil menggunakan mock."""
    # Env variables lengkap
    mock_getenv.side_effect = lambda key: {
        "DB_USER"       : "user",
        "DB_PASSWORD"   : "pass",
        "DB_HOST"       : "localhost",
        "DB_PORT"       : "5433",
        "DB_NAME"       : "untuktestdb"
    }.get(key)

    mock_conn = MagicMock()
    mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn

    save_to_postgres(sample_df, "test_table")

    mock_conn.execute.assert_called()
    mock_conn.commit.assert_called()


@patch("utils.load.os.getenv")
def test_save_to_postgres_missing_env(mock_getenv, sample_df):
    """Test penyimpanan DataFrame ke PostgreSQL gagal ketika env variables tidak lengkap."""
    # Env variables tidak lengkap
    mock_getenv.return_value = None
    with pytest.raises(EnvironmentError, match="Environment variable"):
        save_to_postgres(sample_df, "test_table")


@patch("utils.load.create_engine")
@patch("utils.load.os.getenv")
def test_save_to_postgres_insert_failure(mock_getenv, mock_engine, sample_df):
    """Test penyimpanan DataFrame ke PostgreSQL gagal ketika terjadi exception saat insert."""
    # Env variables lengkap
    mock_getenv.side_effect = lambda key: {
        "DB_USER"       : "user",
        "DB_PASSWORD"   : "pass",
        "DB_HOST"       : "localhost",
        "DB_PORT"       : "5433",
        "DB_NAME"       : "cobacobadb"
    }.get(key)

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("DB Insert Error")
    mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn

    with pytest.raises(Exception, match="DB Insert Error"):
        save_to_postgres(sample_df, "test_table")
