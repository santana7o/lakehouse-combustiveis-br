"""Descobre e baixa os CSVs mensais publicados pela ANP.

A ANP não mantém um padrão de nomes (ex.: `precos-gasolina-etanol-01.csv`,
`01-dados-abertos-precos-gasolina-etanol.csv`, `02-cados-abertos-...`), então
em vez de montar URLs à mão lemos a página oficial e filtramos os links.
"""

import re
import urllib.request
from pathlib import Path

PAGINA_ANP = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
    "serie-historica-de-precos-de-combustiveis"
)

_LINK_MENSAL = re.compile(
    r'href="(?P<url>https://[^"]*/shpc/dsan/(?P<ano>\d{4})/[^"]*gasolina-etanol[^"]*\.csv)"'
)


def listar_arquivos_mensais(html: str, ano_inicial: int) -> list[dict]:
    """Extrai os CSVs mensais de gasolina/etanol a partir do HTML da página."""
    encontrados = {}
    for m in _LINK_MENSAL.finditer(html):
        ano = int(m.group("ano"))
        if ano >= ano_inicial:
            url = m.group("url")
            encontrados[url] = {"url": url, "ano": ano, "nome": f"{ano}_{url.rsplit('/', 1)[1]}"}
    return sorted(encontrados.values(), key=lambda a: a["nome"])


def baixar_novos(destino: str, ano_inicial: int, timeout: int = 120) -> list[str]:
    """Baixa para `destino` (ex.: um Volume do Unity Catalog) só o que ainda não existe.

    Idempotente: rodar duas vezes não duplica arquivos, e o Auto Loader na
    camada Bronze processa cada arquivo exatamente uma vez.
    """
    pasta = Path(destino)
    # Só a subpasta: em Volumes do UC, mkdir na raiz do volume/schema dá EOPNOTSUPP.
    if not pasta.exists():
        pasta.mkdir()
    html = urllib.request.urlopen(PAGINA_ANP, timeout=timeout).read().decode("utf-8")

    baixados = []
    for arquivo in listar_arquivos_mensais(html, ano_inicial):
        alvo = pasta / arquivo["nome"]
        if alvo.exists():
            continue
        conteudo = urllib.request.urlopen(arquivo["url"], timeout=timeout).read()
        tmp = alvo.with_suffix(".tmp")
        tmp.write_bytes(conteudo)
        tmp.rename(alvo)  # escrita atômica: o Auto Loader nunca lê arquivo pela metade
        baixados.append(arquivo["nome"])
    return baixados
