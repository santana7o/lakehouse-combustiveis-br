import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    sessao = (
        SparkSession.builder.master("local[1]")
        .appName("testes-combustiveis")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .getOrCreate()
    )
    yield sessao
    sessao.stop()
