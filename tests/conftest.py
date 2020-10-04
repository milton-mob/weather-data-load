import pytest
import app.config as conf

@pytest.fixture(scope="session")
def spark():
    spark = conf.get_spark_context()

    return spark