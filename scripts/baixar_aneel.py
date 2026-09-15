#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Baixa a base BRUTA de continuidade da ANEEL (DEC, FEC e limites) e grava em
data/. Nada e calculado aqui: o que sai e o que a API devolveu.

Dataset "Indicadores Coletivos de Continuidade (DEC e FEC)":
  apurados : resource 4493985c-baea-429c-9df5-3030422c71d7
  limites  : resource fd69e1dd-fd66-4269-b60c-cc0b7eb221b4
  dominio  : resource 17fc99b7-e707-4ec4-9553-a43d7a41f7a6

Uso:
    python scripts/baixar_aneel.py                      # DEC, FEC, NumCon, todos os anos
    python scripts/baixar_aneel.py --anos 2024,2025
    python scripts/baixar_aneel.py --indicadores DEC,FEC
    python scripts/baixar_aneel.py --csv-plano          # CSV sem gzip (arquivos grandes)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from typing import Any

import pandas as pd
import requests

BASE = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"
RES_APURADOS = "4493985c-baea-429c-9df5-3030422c71d7"
RES_LIMITES = "fd69e1dd-fd66-4269-b60c-cc0b7eb221b4"
RES_DOMINIO = "17fc99b7-e707-4ec4-9553-a43d7a41f7a6"

PAGE = 10000
TIMEOUT = 120
TENTATIVAS = 6
BACKOFF = 3
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

DIR_DADOS = "data"


# ----------------------------------------------------------------------------
# Coleta
# ----------------------------------------------------------------------------
def nova_sessao() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json",
                      "Connection": "close"})
    return s


def get_pagina(sess: requests.Session, params: dict[str, Any]) -> dict:
    """Uma pagina, com retry/backoff. O portal da ANEEL derruba TLS sob carga."""
    erro = None
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            r = sess.get(BASE, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            payload = r.json()
            if not payload.get("success"):
                raise RuntimeError(f"CKAN retornou erro: {payload}")
            return payload["result"]
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                json.JSONDecodeError) as e:
            erro = e
        except requests.exceptions.HTTPError as e:
            if e.response is None or e.response.status_code not in (429, 500, 502, 503, 504):
                raise
            erro = e
        espera = BACKOFF * (2 ** (tentativa - 1))
        print(f"  [retry {tentativa}/{TENTATIVAS}] {type(erro).__name__} — "
              f"esperando {espera}s", flush=True)
        time.sleep(espera)
    raise RuntimeError(f"Falhou apos {TENTATIVAS} tentativas em "
                       f"offset={params.get('offset')}. Ultimo erro: {erro}")


def ckan_fetch(resource_id: str, filters: dict | None = None,
               fields: list[str] | None = None,
               distinct: bool = False, limite: int | None = None) -> pd.DataFrame:
    """Puxa todos os registros de um resource, paginando por _id."""
    out: list[dict[str, Any]] = []
    offset = 0
    page = limite or PAGE
    total = None
    sess = nova_sessao()
    while True:
        params: dict[str, Any] = {"resource_id": resource_id, "limit": page,
                                  "offset": offset}
        if not distinct:
            params["sort"] = "_id"
        if distinct:
            params["distinct"] = "true"
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = ",".join(fields)
        try:
            res = get_pagina(sess, params)
        except RuntimeError:
            if page <= 1000:
                raise
            page //= 4
            print(f"  reduzindo pagina para {page}", flush=True)
            sess = nova_sessao()
            continue
        if total is None:
            total = res.get("total")
            print(f"  {resource_id[:8]}... total={total}", flush=True)
        recs = res["records"]
        out.extend(recs)
        print(f"    {len(out)} registros", flush=True)
        if limite or len(recs) < page:
            break
        offset += len(recs)
        time.sleep(0.5)          # crawl-delay do portal
    df = pd.DataFrame(out)
    if "_id" in df.columns:
        df = df.drop_duplicates(subset=["_id"])
    return df


def anos_disponiveis() -> list[str]:
    df = ckan_fetch(RES_APURADOS, fields=["AnoIndice"], distinct=True, limite=100)
    return sorted(str(int(a)) for a in df["AnoIndice"].dropna().unique())


# ----------------------------------------------------------------------------
# Gravacao
# ----------------------------------------------------------------------------
def grava(df: pd.DataFrame, nome: str, csv_plano: bool, limiar_gz_mb: float = 8.0):
    """
    Grava parquet + CSV. CSV grande sai comprimido (.csv.gz) para nao inchar o
    repositorio a cada atualizacao; pandas le .gz direto.
    """
    os.makedirs(DIR_DADOS, exist_ok=True)
    saidas = []

    pq = os.path.join(DIR_DADOS, f"{nome}.parquet")
    df.to_parquet(pq, index=False)
    saidas.append(pq)

    # escreve o CSV plano e mede: so comprime se realmente ficar grande
    plano = os.path.join(DIR_DADOS, f"{nome}.csv")
    df.to_csv(plano, index=False, encoding="utf-8-sig")
    usa_gz = (not csv_plano) and os.path.getsize(plano) / 1e6 > limiar_gz_mb
    if usa_gz:
        cs = os.path.join(DIR_DADOS, f"{nome}.csv.gz")
        df.to_csv(cs, index=False, encoding="utf-8-sig", compression="gzip")
        os.remove(plano)
    else:
        cs = plano
    saidas.append(cs)

    # o par oposto pode ter sobrado de uma execucao anterior
    antigo = os.path.join(DIR_DADOS, f"{nome}.csv" if usa_gz else f"{nome}.csv.gz")
    if os.path.exists(antigo):
        os.remove(antigo)

    for p in saidas:
        print(f"  {p}  ({os.path.getsize(p) / 1e6:.1f} MB)", flush=True)
    return saidas


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for bloco in iter(lambda: fh.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()


# ----------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--anos", default="todos",
                    help="ex.: 2024,2025 — ou 'todos' para os anos que a API tiver")
    ap.add_argument("--indicadores", default="DEC,FEC,NumCon",
                    help="SigIndicador dos apurados. 'todos' traz as parcelas "
                         "desagregadas tambem (base bem maior)")
    ap.add_argument("--csv-plano", action="store_true",
                    help="CSV sem gzip, mesmo quando o arquivo for grande")
    args = ap.parse_args()

    anos = anos_disponiveis() if args.anos == "todos" else \
        [a.strip() for a in args.anos.split(",") if a.strip()]
    print(f"anos: {', '.join(anos)}")

    inds = None if args.indicadores == "todos" else \
        [i.strip() for i in args.indicadores.split(",") if i.strip()]
    print(f"indicadores: {', '.join(inds) if inds else 'todos'}\n")

    print("[1/3] apurados ...")
    f_ap: dict[str, Any] = {"AnoIndice": anos}
    if inds:
        f_ap["SigIndicador"] = inds
    apur = ckan_fetch(RES_APURADOS, f_ap)
    apur = apur.drop(columns=[c for c in ["_id"] if c in apur])
    grava(apur, "aneel_continuidade_apurados", args.csv_plano)

    print("\n[2/3] limites ...")
    lim = ckan_fetch(RES_LIMITES, {"AnoLimiteQualidade": anos,
                                   "SigIndicador": ["DEC", "FEC"]})
    lim = lim.drop(columns=[c for c in ["_id"] if c in lim])
    grava(lim, "aneel_continuidade_limites", args.csv_plano)

    print("\n[3/3] dominio de indicadores ...")
    dom = ckan_fetch(RES_DOMINIO)
    dom = dom.drop(columns=[c for c in ["_id", "rank"] if c in dom])
    grava(dom, "aneel_dominio_indicadores", args.csv_plano)

    meta = {
        "baixado_em_utc": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "anos": anos,
        "indicadores": inds or "todos",
        "fonte": {
            "portal": "https://dadosabertos.aneel.gov.br/dataset/"
                      "indicadores-coletivos-de-continuidade-dec-e-fec",
            "resource_apurados": RES_APURADOS,
            "resource_limites": RES_LIMITES,
            "resource_dominio": RES_DOMINIO,
        },
        "linhas": {"apurados": len(apur), "limites": len(lim), "dominio": len(dom)},
        "geracao_na_origem": {
            "apurados": str(apur["DatGeracaoConjuntoDados"].max())
            if "DatGeracaoConjuntoDados" in apur else None,
            "limites": str(lim["DatGeracaoConjuntoDados"].max())
            if "DatGeracaoConjuntoDados" in lim else None,
        },
        "arquivos": {f: {"bytes": os.path.getsize(os.path.join(DIR_DADOS, f)),
                         "sha256": sha256(os.path.join(DIR_DADOS, f))}
                     for f in sorted(os.listdir(DIR_DADOS))
                     if not f.startswith(".") and f != "metadados.json"},
    }
    with open(os.path.join(DIR_DADOS, "metadados.json"), "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)

    print(f"\n[ok] apurados={len(apur)}  limites={len(lim)}  dominio={len(dom)}")
    resumo = os.environ.get("GITHUB_STEP_SUMMARY")
    if resumo:
        with open(resumo, "a", encoding="utf-8") as fh:
            fh.write(f"## Base ANEEL atualizada\n\n"
                     f"| item | valor |\n|---|---|\n"
                     f"| anos | {', '.join(anos)} |\n"
                     f"| indicadores | {', '.join(inds) if inds else 'todos'} |\n"
                     f"| apurados | {len(apur):,} linhas |\n"
                     f"| limites | {len(lim):,} linhas |\n"
                     f"| gerado na origem | "
                     f"{meta['geracao_na_origem']['apurados']} |\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
