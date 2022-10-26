from app.models.data_ingestion_registry import DataNormalizationRegistry


object_time = "Thu, 13 Oct 2022 21:01:50 GMT"


def test_convert_gmt_time():
    assert DataNormalizationRegistry.convert_gmt_time(object_time) == {
        "last_modified": "2022-10-13 14:01:50-07:00 PST"
    }
