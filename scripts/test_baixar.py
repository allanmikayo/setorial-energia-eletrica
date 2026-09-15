# -*- coding: utf-8 -*-
"""
Testes do downloader, com a camada de rede simulada (não faz requisição real).

Cobre: paginação da API, retry, leitura do arquivo-fonte (parquet e CSV em
vários separadores/codificações), filtro por ano e indicador, queda automática
do arquivo para a API, gravação parquet/CSV e metadados.
"""
import glob
import gzip
import json
import os
import shutil
import sys
import tempfile

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(__file__))
import baixar_aneel as B

B.BACKOFF = 0.01
TMP = tempfile.mkdtemp()
B.DIR_DADOS = os.path.join(TMP, "data")
ORIG = {"nova_sessao": B.nova_sessao, "meta_recurso": B.meta_recurso,
        "baixa_arquivo": B.baixa_arquivo}

ANOS_API = ["2022", "2023", "2024", "2025"]        # o que a API tem hoje
ANOS_ARQ = ["2020", "2021"] + ANOS_API + ["2026"]  # o arquivo tem o ano corrente


def linhas_apurados(anos, n_por_ano=60):
    out = []
    for a in anos:
        for i in range(n_por_ano):
            out.append({"DatGeracaoConjuntoDados": "2026-09-05T00:00:00.000",
                        "SigAgente": "CEMIG-D", "NumCNPJ": "6981180000116",
                        "IdeConjUndConsumidoras": str(15000 + (i % 5)),
                        "DscConjUndConsumidoras": "Conjunto X",
                        # indicador e mes independentes: se andarem juntos, o
                        # filtro por indicador derruba meses inteiros sem querer
                        "SigIndicador": ["DEC", "FEC", "NumCon", "DECIP"][(i // 12) % 4],
                        "AnoIndice": a,
                        "NumPeriodoIndice": str((i % 12) + 1),
                        "VlrIndiceEnviado": ",07"})
    return pd.DataFrame(out)


def linhas_limites(anos):
    out = []
    for a in anos:
        for i in range(10):
            out.append({"DatGeracaoConjuntoDados": "2026-09-05T00:00:00.000",
                        "SigAgente": "CEMIG-D", "NumCNPJ": "6981180000116",
                        "IdeConjUndConsumidoras": str(15000 + (i % 5)),
                        "DscConjUndConsumidoras": "Conjunto X",
                        "SigIndicador": ["DEC", "FEC"][i % 2],
                        "AnoLimiteQualidade": a, "VlrLimite": "7,00"})
    return pd.DataFrame(out)


# ============================================================================
# 1. Leitura do arquivo-fonte: parquet e CSV em varios dialetos
# ============================================================================
pq = os.path.join(TMP, "a.parquet")
linhas_apurados(ANOS_ARQ).to_parquet(pq, index=False)
df = B.le_tabela(pq, B.COLS_APURADOS)
assert len(df) == 60 * len(ANOS_ARQ)
print("ok  le parquet e confere as colunas esperadas")

for sep, enc in ((";", "utf-8-sig"), (",", "utf-8-sig"), (";", "latin-1")):
    p = os.path.join(TMP, f"l_{sep}_{enc}.csv")
    linhas_limites(ANOS_ARQ).to_csv(p, sep=sep, encoding=enc, index=False)
    d = B.le_tabela(p, B.COLS_LIMITES)
    assert len(d) == 10 * len(ANOS_ARQ), (sep, enc, len(d))
    assert d["VlrLimite"].iloc[0] == "7,00"
print("ok  le CSV em ';'/',' e utf-8/latin-1, mantendo o texto pt-BR intacto")

import zipfile
pz = os.path.join(TMP, "z.zip")
csv_interno = os.path.join(TMP, "interno.csv")
linhas_limites(["2025"]).to_csv(csv_interno, sep=";", index=False)
with zipfile.ZipFile(pz, "w") as z:
    z.write(csv_interno, "interno.csv")
assert len(B.le_tabela(pz, B.COLS_LIMITES)) == 10
print("ok  le zip com csv dentro")

ruim = os.path.join(TMP, "ruim.csv")
pd.DataFrame({"outra": [1, 2]}).to_csv(ruim, index=False)
try:
    B.le_tabela(ruim, B.COLS_LIMITES)
    raise AssertionError("deveria ter recusado")
except ValueError as e:
    assert "nao consegui interpretar" in str(e)
print("ok  arquivo com colunas erradas vira erro explicito, nao tabela silenciosa")


# ============================================================================
# 2. Filtro por ano e indicador
# ============================================================================
bruto = linhas_apurados(ANOS_ARQ)
f = B.filtra(bruto, "AnoIndice", ["2025", "2026"], ["DEC", "FEC", "NumCon"])
assert set(f["AnoIndice"]) == {"2025", "2026"}
assert set(f["SigIndicador"]) == {"DEC", "FEC", "NumCon"}, set(f["SigIndicador"])
assert "DECIP" not in set(f["SigIndicador"])
print("ok  filtro por ano e indicador")
assert len(B.filtra(bruto, "AnoIndice", None, None)) == len(bruto)
print("ok  'todos' nao filtra nada — o ano corrente sobrevive")


# ============================================================================
# 3. Fonte "arquivo": download simulado ponta a ponta
# ============================================================================
def fake_meta(rid):
    if rid == B.ARQ_APURADOS_PARQUET:
        return {"url": "https://x/ap.parquet", "formato": "PARQUET",
                "modificado_em": "2026-09-05T05:16:00", "bytes": 30197991,
                "nome": "indicadores-continuidade-coletivos-2020-2029.parquet"}
    return {"url": "https://x/lim.csv", "formato": "CSV",
            "modificado_em": "2026-09-05T05:27:08", "bytes": 25865955,
            "nome": "indicadores-continuidade-coletivos-limite"}


def fake_download(url, destino):
    if url.endswith(".parquet"):
        linhas_apurados(ANOS_ARQ).to_parquet(destino, index=False)
    else:
        linhas_limites(ANOS_ARQ).to_csv(destino, sep=";", index=False,
                                        encoding="latin-1")
    return destino


B.meta_recurso = fake_meta
B.baixa_arquivo = fake_download

ap, fonte, meta = B.carrega_apurados("arquivo", None, ["DEC", "FEC", "NumCon"], TMP)
assert fonte == "arquivo"
assert "2026" in set(ap["AnoIndice"]), sorted(set(ap["AnoIndice"]))
assert "2020" in set(ap["AnoIndice"])
assert meta["modificado_em"] == "2026-09-05T05:16:00"
print("ok  fonte 'arquivo' traz 2020 e 2026 — anos que a API nao entrega")

li, fonte_li, _ = B.carrega_limites("arquivo", None, TMP)
assert fonte_li == "arquivo" and set(li["SigIndicador"]) == {"DEC", "FEC"}
print("ok  limites pelo arquivo, so DEC e FEC")


# ============================================================================
# 4. Queda automatica para a API quando o arquivo falha
# ============================================================================
falhas = {"n": 0}


class FakeSess:
    headers = {}

    def get(self, url, params=None, timeout=None, stream=None):
        falhas["n"] += 1
        if falhas["n"] == 2:                     # uma queda de TLS no meio
            raise requests.exceptions.SSLError("portal caiu")
        off, lim = params["offset"], params["limit"]
        res = params["resource_id"]
        if res == B.RES_DOMINIO:
            tot, base = 23, [{"_id": i, "SigIndicador": f"I{i}",
                              "DscIndicador": "x", "rank": 0.1} for i in range(23)]
        else:
            src = linhas_apurados(ANOS_API) if res == B.RES_APURADOS \
                else linhas_limites(ANOS_API)
            base = src.to_dict("records")
            for i, r in enumerate(base):
                r["_id"] = i
            tot = len(base)
        recs = base[off:off + lim]

        class R:
            @staticmethod
            def raise_for_status():
                pass

            @staticmethod
            def json():
                return {"success": True, "result": {"total": tot, "records": recs}}
        return R()


B.nova_sessao = lambda: FakeSess()
B.PAGE = 100


def download_quebrado(url, destino):
    raise RuntimeError("nao foi possivel baixar: portal derrubou a conexao")


B.baixa_arquivo = download_quebrado
ap2, fonte2, _ = B.carrega_apurados("auto", None, ["DEC", "FEC", "NumCon"], TMP)
assert fonte2 == "api", fonte2
assert set(ap2["AnoIndice"]) == set(ANOS_API), sorted(set(ap2["AnoIndice"]))
assert "2026" not in set(ap2["AnoIndice"])
print("ok  'auto': arquivo falha -> cai para a API (e a API realmente nao tem 2026)")

try:
    B.carrega_apurados("arquivo", None, None, TMP)
    raise AssertionError("deveria ter falhado")
except RuntimeError:
    print("ok  'arquivo' explicito nao mascara a falha, propaga o erro")


# ============================================================================
# 5. main() ponta a ponta com a fonte arquivo
# ============================================================================
B.baixa_arquivo = fake_download
shutil.rmtree(B.DIR_DADOS, ignore_errors=True)
resumo = os.path.join(TMP, "summary.md")
os.environ["GITHUB_STEP_SUMMARY"] = resumo
sys.argv = ["x", "--fonte", "arquivo"]
assert B.main() == 0

meta = json.load(open(os.path.join(B.DIR_DADOS, "metadados.json"), encoding="utf-8"))
assert meta["fonte"]["apurados"] == "arquivo"
assert meta["cobertura"]["anos_apurados"] == ANOS_ARQ, meta["cobertura"]["anos_apurados"]
assert meta["cobertura"]["ultimo_mes_apurado"] == "2026-12", \
    meta["cobertura"]["ultimo_mes_apurado"]
assert meta["cobertura"]["meses_presentes"] == list(range(1, 13))
assert meta["origem"]["recurso_apurados_modificado_em"] == "2026-09-05T05:16:00"
assert all(len(v["sha256"]) == 64 for v in meta["arquivos"].values())
assert len(meta["arquivos"]) == 6, list(meta["arquivos"])
print("ok  main() grava 6 arquivos + metadados com cobertura e ultimo mes apurado")

txt = open(resumo, encoding="utf-8").read()
assert "último mês apurado" in txt and "2026-12" in txt
print("ok  resumo do Actions mostra o ultimo mes apurado")

p = pd.read_parquet(os.path.join(B.DIR_DADOS, "aneel_continuidade_apurados.parquet"))
assert p["VlrIndiceEnviado"].iloc[0] == ",07"
assert "2026" in set(p["AnoIndice"].astype(str))
print("ok  parquet final tem 2026 e o texto bruto intacto")

# aviso quando cai para a API
open(resumo, "w").close()
shutil.rmtree(B.DIR_DADOS, ignore_errors=True)
B.baixa_arquivo = download_quebrado
sys.argv = ["x", "--fonte", "auto"]
assert B.main() == 0
assert "caiu para a API" in open(resumo, encoding="utf-8").read()
print("ok  quando cai para a API, o resumo avisa que faltam anos")

# CSV grande comprime, pequeno nao
B.baixa_arquivo = fake_download
B.grava(linhas_apurados(ANOS_ARQ * 40), "teste_grande", csv_plano=False, limiar_gz_mb=0.5)
assert os.path.exists(os.path.join(B.DIR_DADOS, "teste_grande.csv.gz"))
with gzip.open(os.path.join(B.DIR_DADOS, "teste_grande.csv.gz"), "rt",
               encoding="utf-8-sig") as fh:
    assert len(pd.read_csv(fh)) == 60 * len(ANOS_ARQ) * 40
print("ok  CSV grande vira .csv.gz e reabre integro")

B.nova_sessao, B.meta_recurso, B.baixa_arquivo = (ORIG["nova_sessao"],
                                                  ORIG["meta_recurso"],
                                                  ORIG["baixa_arquivo"])
print("\narquivos:", sorted(os.path.basename(x) for x in glob.glob(B.DIR_DADOS + "/*")))
shutil.rmtree(TMP, ignore_errors=True)
print("\nTODOS OS TESTES PASSARAM")
