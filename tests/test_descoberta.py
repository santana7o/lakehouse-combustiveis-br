from combustiveis.descoberta import listar_arquivos_mensais

BASE = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc"

HTML = f"""
<a href="{BASE}/dsan/2024/precos-gasolina-etanol-12.csv">dez/24</a>
<a href="{BASE}/dsan/2025/precos-gasolina-etanol-01.csv">jan/25</a>
<a href="{BASE}/dsan/2025/precos-diesel-gnv-01.csv">diesel</a>
<a href="{BASE}/dsan/2026/02-cados-abertos-preco-gasolina-etanol.csv">fev/26 (typo da ANP)</a>
<a href="{BASE}/dsan/2026/02-cados-abertos-preco-gasolina-etanol.csv">duplicado</a>
<a href="{BASE}/qus/ultimas-4-semanas-gasolina-etanol.csv">últimas 4 semanas</a>
"""


def test_filtra_gasolina_etanol_mensal_a_partir_do_ano():
    nomes = [a["nome"] for a in listar_arquivos_mensais(HTML, ano_inicial=2025)]
    assert nomes == [
        "2025_precos-gasolina-etanol-01.csv",
        "2026_02-cados-abertos-preco-gasolina-etanol.csv",
    ]
