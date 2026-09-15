# -*- coding: utf-8 -*-
"""Testa o downloader com a camada HTTP simulada (sem rede)."""
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

ANOS = ["2022", "2023", "2024", "2025"]
falhas = {"n": 0}


def fake_records(res, params):
    off, lim = params["offset"], params["limit"]
    if params.get("distinct"):
        return [{"AnoIndice": int(a)} for a in ANOS], len(ANOS)
    if res == B.RES_DOMINIO:
        tot = 23
        return [{"_id": i, "SigIndicador": f"IND{i}", "DscIndicador": "x", "rank": 0.1}
                for i in range(off, min(off + lim, tot))], tot
    tot = 25000 if res == B.RES_APURADOS else 800
    recs = []
    for i in range(off, min(off + lim, tot)):
        recs.append({"_id": i, "DatGeracaoConjuntoDados": "2026-06-05T00:00:00.000",
                     "SigAgente": "CEMIG-D", "NumCNPJ": 6981180000116,
                     "IdeConjUndConsumidoras": 15000 + (i % 300),
                     "DscConjUndConsumidoras": "Conjunto X",
                     "SigIndicador": ["DEC", "FEC", "NumCon"][i % 3],
                     "AnoIndice": 2025, "NumPeriodoIndice": (i % 12) + 1,
                     "VlrIndiceEnviado": ",07"})
    return recs, tot


class FakeSess:
    headers = {}

    def get(self, url, params=None, timeout=None):
        # duas quedas de TLS no comeco, para exercitar o retry
        falhas["n"] += 1
        if falhas["n"] in (2, 3):
            raise requests.exceptions.SSLError("portal caiu")
        recs, tot = fake_records(params["resource_id"], params)

        class R:
            @staticmethod
            def raise_for_status():
                pass

            @staticmethod
            def json():
                return {"success": True, "result": {"total": tot, "records": recs}}
        return R()


B.nova_sessao = lambda: FakeSess()

# --- descoberta de anos ------------------------------------------------------
assert B.anos_disponiveis() == ANOS, B.anos_disponiveis()
print("ok  --anos todos descobre os anos pela API:", ANOS)

# --- paginacao ---------------------------------------------------------------
df = B.ckan_fetch(B.RES_APURADOS, {"AnoIndice": ANOS})
assert len(df) == 25000, len(df)
assert df["_id"].is_unique and df["_id"].min() == 0 and df["_id"].max() == 24999
print("ok  paginacao completa e sem duplicata, com 2 quedas de TLS recuperadas:",
      len(df), "linhas")

# --- gravacao: CSV grande vira .gz, pequeno fica plano -----------------------
B.grava(df.drop(columns=["_id"]), "aneel_continuidade_apurados", csv_plano=False,
        limiar_gz_mb=1.0)
arqs = sorted(os.path.basename(p) for p in glob.glob(B.DIR_DADOS + "/*"))
assert "aneel_continuidade_apurados.parquet" in arqs
assert "aneel_continuidade_apurados.csv.gz" in arqs, arqs
print("ok  base grande ->", [a for a in arqs if "apurados" in a])

peq = df.head(50).drop(columns=["_id"])
B.grava(peq, "aneel_continuidade_limites", csv_plano=False)
assert os.path.exists(os.path.join(B.DIR_DADOS, "aneel_continuidade_limites.csv"))
print("ok  base pequena mantem CSV plano")

# --- conteudo: o CSV.gz reabre igual ao parquet ------------------------------
p = pd.read_parquet(os.path.join(B.DIR_DADOS, "aneel_continuidade_apurados.parquet"))
with gzip.open(os.path.join(B.DIR_DADOS, "aneel_continuidade_apurados.csv.gz"), "rt",
               encoding="utf-8-sig") as fh:
    c = pd.read_csv(fh)
assert len(p) == len(c) == 25000 and list(p.columns) == list(c.columns)
assert c["VlrIndiceEnviado"].iloc[0] == ",07", c["VlrIndiceEnviado"].iloc[0]
assert "_id" not in p.columns
print("ok  parquet e csv.gz batem, e o texto bruto ',07' chega intacto")

# --- troca de formato remove o arquivo antigo --------------------------------
B.grava(df.drop(columns=["_id"]), "aneel_continuidade_apurados", csv_plano=True)
assert os.path.exists(os.path.join(B.DIR_DADOS, "aneel_continuidade_apurados.csv"))
assert not os.path.exists(os.path.join(B.DIR_DADOS, "aneel_continuidade_apurados.csv.gz"))
print("ok  --csv-plano troca .csv.gz por .csv sem deixar sobra")

# --- main() ponta a ponta, com o resumo do GitHub ----------------------------
shutil.rmtree(B.DIR_DADOS, ignore_errors=True)
resumo = os.path.join(TMP, "summary.md")
os.environ["GITHUB_STEP_SUMMARY"] = resumo
sys.argv = ["x", "--anos", "2025", "--indicadores", "DEC,FEC,NumCon"]
assert B.main() == 0
meta = json.load(open(os.path.join(B.DIR_DADOS, "metadados.json"), encoding="utf-8"))
assert meta["linhas"]["apurados"] == 25000 and meta["linhas"]["limites"] == 800
assert meta["linhas"]["dominio"] == 23
assert meta["anos"] == ["2025"]
assert len(meta["arquivos"]) == 6, list(meta["arquivos"])
assert all(len(v["sha256"]) == 64 for v in meta["arquivos"].values())
assert meta["geracao_na_origem"]["apurados"].startswith("2026-06-05")
print("ok  main() gera 6 arquivos + metadados.json com sha256 e data de geracao")
assert "Base ANEEL atualizada" in open(resumo, encoding="utf-8").read()
print("ok  resumo do GitHub Actions escrito")
print("\narquivos finais:", sorted(os.path.basename(p) for p in glob.glob(B.DIR_DADOS + "/*")))
shutil.rmtree(TMP, ignore_errors=True)
print("\nTODOS OS TESTES PASSARAM")
