import pytest
from pyspark.sql import SparkSession
from analysis import top_non_legendary, type_with_highest_avg_hp, most_common_special_attack

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def mock_data(spark_session):
    data = [
        ("A", "Fire", None, 100, 50, 20, 10, 60, 5, 15, 1, False),
        ("B", "Water", "Flying", 110, 55, 22, 11, 61, 6, 16, 1, False),
        ("C", "Grass", None, 111, 56, 23, 12, 62, 7, 17, 2, True),
        ("D", "Fire", "Dragon", 112, 57, 24, 13, 63, 8, 18, 1, False),
        ("E", "Water", "Poison", 113, 58, 25, 14, 64, 9, 19, 1, False),
        ("F", "Fire", None, 120, 50, 26, 15, 60, 10, 20, 1, True),
    ]
    columns = ["Name", "Type 1", "Type 2", "Total", "HP", "Attack", "Defense", "Sp. Atk", "Sp. Def", "Speed", "Generation", "Legendary"]
    return spark_session.createDataFrame(data, schema=columns)

def test_top_non_legendary(mock_data):
    result = top_non_legendary(mock_data)
    assert result.count() == 4 

def test_type_with_highest_avg_hp(mock_data):
    result = type_with_highest_avg_hp(mock_data)
    assert result.count() == 1

def test_most_common_special_attack(mock_data):
    result = most_common_special_attack(mock_data)
    assert result.count() == 1
