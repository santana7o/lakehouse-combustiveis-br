from datetime import date
from decimal import Decimal

from combustiveis import transformacoes as t

CABECALHO = list(t.COLUNAS_ANP)
CABECALHO[0] = "\ufeff" + CABECALHO[0]  # o CSV real vem com BOM


def linha(
    uf="SP",
    produto="GASOLINA",
    data="16/09/2026",
    preco="5,99",
    cnpj=" 88.587.589/0001-17",
    bandeira="BRANCA",
):
    return (
        "SE",
        uf,
        "SAO PAULO",
        "POSTO X",
        cnpj,
        "RUA A",
        "1",
        "",
        "CENTRO",
        "01000-000",
        produto,
        data,
        preco,
        "",
        "R$ / litro",
        bandeira,
    )


def bronze(spark, linhas):
    return t.normalizar_colunas(spark.createDataFrame(linhas, CABECALHO))


def test_normalizar_colunas_remove_bom_e_usa_snake_case(spark):
    df = bronze(spark, [linha()])
    assert df.columns == list(t.COLUNAS_ANP.values())


def test_limpar_precos_tipa_valores_brasileiros(spark):
    r = t.limpar_precos(bronze(spark, [linha()])).first()
    assert r.valor_venda == Decimal("5.990")
    assert r.valor_compra is None
    assert r.data_coleta == date(2026, 9, 16)
    assert r.semana == date(2026, 9, 14)  # segunda-feira
    assert r.cnpj_revenda == "88587589000117"
    assert r.cep == "01000000"


def test_limpar_precos_remove_coleta_duplicada(spark):
    df = t.limpar_precos(bronze(spark, [linha(), linha(), linha(produto="ETANOL")]))
    assert df.count() == 2


def test_limpar_precos_dedup_prefere_preco_valido(spark):
    df = t.limpar_precos(bronze(spark, [linha(preco="99,00"), linha(preco="5,79")]))
    assert df.count() == 1
    assert df.first().valor_venda == Decimal("5.790")


def test_paridade_etanol(spark):
    linhas = [
        linha(uf="SP", produto="GASOLINA", preco="6,00"),
        linha(uf="SP", produto="ETANOL", preco="3,90", cnpj="11.111.111/0001-11"),
        linha(uf="RS", produto="GASOLINA", preco="6,00", cnpj="33.333.333/0001-33"),
        linha(uf="RS", produto="ETANOL", preco="4,80", cnpj="22.222.222/0001-22"),
    ]
    semanal = t.preco_semanal_por_uf(t.limpar_precos(bronze(spark, linhas)))
    res = {r.uf: r for r in t.paridade_etanol_gasolina(semanal).collect()}
    assert res["SP"].paridade == 0.65 and res["SP"].etanol_compensa
    assert res["RS"].paridade == 0.8 and not res["RS"].etanol_compensa


def test_ranking_bandeiras_mais_barata_primeiro(spark):
    linhas = [
        linha(bandeira="VIBRA", preco="6,20", cnpj="11.111.111/0001-11"),
        linha(bandeira="BRANCA", preco="5,80", cnpj="22.222.222/0001-22"),
        linha(bandeira="IPIRANGA", preco="6,00", cnpj="33.333.333/0001-33"),
    ]
    ranking = t.ranking_bandeiras(t.limpar_precos(bronze(spark, linhas)))
    ordem = [r.bandeira for r in ranking.orderBy("ranking").collect()]
    assert ordem == ["BRANCA", "IPIRANGA", "VIBRA"]
