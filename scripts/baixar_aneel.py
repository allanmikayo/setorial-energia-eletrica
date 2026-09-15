#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Baixa a base BRUTA de continuidade da ANEEL (DEC, FEC e limites) e grava em
data/. Nada e calculado aqui: o que sai e o que a API devolveu.

Dataset "Indicadores Coletivos de Continuidade (DEC e FEC)":
  apurados : resource 4493985c-baea-429c-9df5-3030422c71d7
  limites  : resource fd69e1dd-fd66-4269-b60c-cc0b7eb221b4
  dominio  : resource 17fc99b7-e707-4ec4-9553-a43d7a41f7a6

Fonte padrao e o ARQUIVO-FONTE completo do portal, nao a API: o datastore da
API nao carrega o arquivo inteiro e hoje para em 2025, enquanto o arquivo ja
tem o ano corrente. A API fica como reserva automatica.

Uso:
    python scripts/baixar_aneel.py                      # arquivo completo, DEC/FEC/NumCon
    python scripts/baixar_aneel.py --anos 2024,2025
    python scripts/baixar_aneel.py --fonte api          # forca a API paginada
    python scripts/baixar_aneel.py --indicadores todos
    python scripts/baixar_aneel.py --csv-plano          # CSV sem gzip
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from typing import Any

import pandas as pd
import requests

BASE = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"
RESOURCE_SHOW = "https://dadosabertos.aneel.gov.br/api/3/action/resource_show"

# Recursos do datastore (API paginada).
RES_APURADOS = "4493985c-baea-429c-9df5-3030422c71d7"
RES_LIMITES = "fd69e1dd-fd66-4269-b60c-cc0b7eb221b4"
RES_DOMINIO = "17fc99b7-e707-4ec4-9553-a43d7a41f7a6"

# Arquivos-fonte completos. O datastore da API NAO carrega o arquivo inteiro
# (o proprio metadado do portal traz datastore_contains_all_records_of_source_file
# = false para os apurados): hoje a API so devolve 2022-2025, enquanto o arquivo
# vai de 2020 em diante e ja tem o ano corrente. Por isso a fonte padrao e o
# arquivo, com a API como reserva.
ARQ_APURADOS_PARQUET = "d7f70fb1-725c-4748-afeb-65c6a78df550"  # 2020-2029
ARQ_APURADOS_ANTIGO = "1706a88f-ecd6-4de9-99ee-ec240c317378"   # 2010-2019

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
# Fonte "arquivo": baixa o arquivo-fonte completo do portal
# ----------------------------------------------------------------------------
COLS_APURADOS = {"SigAgente", "IdeConjUndConsumidoras", "SigIndicador",
                 "AnoIndice", "NumPeriodoIndice", "VlrIndiceEnviado"}
COLS_LIMITES = {"SigAgente", "IdeConjUndConsumidoras", "SigIndicador",
                "AnoLimiteQualidade", "VlrLimite"}


def meta_recurso(resource_id: str) -> dict:
    """URL de download e data de modificacao de um recurso do portal."""
    sess = nova_sessao()
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            r = sess.get(RESOURCE_SHOW, params={"id": resource_id}, timeout=TIMEOUT)
            r.raise_for_status()
            res = r.json()["result"]
            return {"url": res["url"], "formato": (res.get("format") or "").upper(),
                    "modificado_em": res.get("last_modified"),
                    "bytes": res.get("size"), "nome": res.get("name")}
        except Exception as e:
            if tentativa == TENTATIVAS:
                raise
            espera = BACKOFF * (2 ** (tentativa - 1))
            print(f"  [retry {tentativa}] resource_show: {type(e).__name__} — "
                  f"{espera}s", flush=True)
            time.sleep(espera)
    raise RuntimeError("inalcancavel")


def baixa_arquivo(url: str, destino: str) -> str:
    """
    Download em streaming, com retry. O portal derruba transferencia grande com
    alguma frequencia, entao cada tentativa recomeca o arquivo do zero e o
    tamanho e conferido contra o Content-Length antes de dar por bom.
    """
    erro = None
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            sess = nova_sessao()
            with sess.get(url, stream=True, timeout=TIMEOUT) as r:
                r.raise_for_status()
                esperado = r.headers.get("Content-Length")
                esperado = int(esperado) if esperado and esperado.isdigit() else None
                baixado = 0
                with open(destino, "wb") as fh:
                    for bloco in r.iter_content(chunk_size=1 << 20):
                        if bloco:
                            fh.write(bloco)
                            baixado += len(bloco)
            if esperado and baixado != esperado:
                raise IOError(f"download incompleto: {baixado} de {esperado} bytes")
            print(f"  baixado: {baixado / 1e6:.1f} MB", flush=True)
            return destino
        except Exception as e:
            erro = e
            if os.path.exists(destino):
                os.remove(destino)
            if tentativa == TENTATIVAS:
                break
            espera = BACKOFF * (2 ** (tentativa - 1))
            print(f"  [retry {tentativa}/{TENTATIVAS}] download: "
                  f"{type(e).__name__} — {espera}s", flush=True)
            time.sleep(espera)
    raise RuntimeError(f"nao foi possivel baixar {url}: {erro}")


def le_tabela(path: str, colunas_esperadas: set[str]) -> pd.DataFrame:
    """
    Le parquet, csv ou zip-com-csv. O CSV da ANEEL varia em separador e
    codificacao entre recursos, entao testa as combinacoes e aceita a primeira
    que traga as colunas esperadas — assim uma mudanca na origem vira erro
    explicito, nao uma tabela silenciosamente errada.
    """
    baixo = path.lower()
    if baixo.endswith(".parquet"):
        df = pd.read_parquet(path)
        faltando = colunas_esperadas - set(df.columns)
        if faltando:
            raise ValueError(f"{path}: faltam colunas {sorted(faltando)}")
        return df

    alvo = path
    tmpzip = None
    if baixo.endswith(".zip"):
        import zipfile
        with zipfile.ZipFile(path) as z:
            nomes = [n for n in z.namelist() if n.lower().endswith((".csv", ".txt"))]
            if not nomes:
                raise ValueError(f"{path}: zip sem csv dentro ({z.namelist()})")
            tmpzip = z.extract(nomes[0], os.path.dirname(path) or ".")
            alvo = tmpzip

    erros = []
    for sep in (";", ","):
        for enc in ("utf-8-sig", "latin-1"):
            try:
                df = pd.read_csv(alvo, sep=sep, encoding=enc, dtype=str,
                                 low_memory=False)
            except Exception as e:
                erros.append(f"sep={sep!r} enc={enc}: {type(e).__name__}")
                continue
            if colunas_esperadas <= set(df.columns):
                print(f"  lido com sep={sep!r} encoding={enc}: "
                      f"{len(df)} linhas", flush=True)
                if tmpzip and os.path.exists(tmpzip):
                    os.remove(tmpzip)
                return df
            erros.append(f"sep={sep!r} enc={enc}: colunas {list(df.columns)[:6]}")
    if tmpzip and os.path.exists(tmpzip):
        os.remove(tmpzip)
    raise ValueError(f"{path}: nao consegui interpretar o CSV. Tentativas: {erros}")


def baixa_recurso_como_df(resource_id: str, colunas: set[str],
                          tmpdir: str) -> tuple[pd.DataFrame, dict]:
    meta = meta_recurso(resource_id)
    print(f"  {meta['nome']} ({meta['formato']}, "
          f"modificado em {meta['modificado_em']})", flush=True)
    ext = os.path.splitext(meta["url"].split("?")[0])[1] or ".dat"
    destino = os.path.join(tmpdir, f"{resource_id}{ext}")
    baixa_arquivo(meta["url"], destino)
    df = le_tabela(destino, colunas)
    try:
        os.remove(destino)
    except OSError:
        pass
    return df, meta


def filtra(df: pd.DataFrame, col_ano: str, anos: list[str] | None,
           inds: list[str] | None) -> pd.DataFrame:
    if anos:
        df = df[df[col_ano].astype(str).str.strip().str[:4].isin(anos)]
    if inds and "SigIndicador" in df.columns:
        df = df[df["SigIndicador"].astype(str).str.strip().isin(inds)]
    return df.reset_index(drop=True)


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
def carrega_apurados(fonte: str, anos, inds, tmpdir: str):
    """Arquivo-fonte completo (padrao) com a API paginada como reserva."""
    if fonte in ("arquivo", "auto"):
        try:
            print("[1/3] apurados — arquivo-fonte completo ...")
            df, meta = baixa_recurso_como_df(ARQ_APURADOS_PARQUET, COLS_APURADOS,
                                             tmpdir)
            return filtra(df, "AnoIndice", anos, inds), "arquivo", meta
        except Exception as e:
            if fonte == "arquivo":
                raise
            print(f"  [aviso] arquivo falhou ({type(e).__name__}: {e}).\n"
                  f"          Caindo para a API, que hoje cobre menos anos.",
                  flush=True)

    print("[1/3] apurados — API paginada ...")
    f: dict[str, Any] = {}
    if anos:
        f["AnoIndice"] = anos
    if inds:
        f["SigIndicador"] = inds
    df = ckan_fetch(RES_APURADOS, f or None)
    return df.drop(columns=[c for c in ["_id"] if c in df]), "api", {}


def carrega_limites(fonte: str, anos, tmpdir: str):
    if fonte in ("arquivo", "auto"):
        try:
            print("\n[2/3] limites — arquivo-fonte completo ...")
            df, meta = baixa_recurso_como_df(RES_LIMITES, COLS_LIMITES, tmpdir)
            return (filtra(df, "AnoLimiteQualidade", anos, ["DEC", "FEC"]),
                    "arquivo", meta)
        except Exception as e:
            if fonte == "arquivo":
                raise
            print(f"  [aviso] arquivo falhou ({type(e).__name__}: {e}). "
                  f"Caindo para a API.", flush=True)

    print("\n[2/3] limites — API paginada ...")
    f: dict[str, Any] = {"SigIndicador": ["DEC", "FEC"]}
    if anos:
        f["AnoLimiteQualidade"] = anos
    df = ckan_fetch(RES_LIMITES, f)
    return df.drop(columns=[c for c in ["_id"] if c in df]), "api", {}


def anos_presentes(df: pd.DataFrame, col: str) -> list[str]:
    if col not in df.columns:
        return []
    s = df[col].astype(str).str.strip().str[:4]
    return sorted(a for a in s.dropna().unique() if a.isdigit())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fonte", default="auto", choices=["auto", "arquivo", "api"],
                    help="'arquivo' baixa o arquivo-fonte completo do portal "
                         "(traz o ano corrente); 'api' pagina o datastore, que "
                         "hoje so vai ate 2025; 'auto' tenta o arquivo e cai "
                         "para a API se ele falhar")
    ap.add_argument("--anos", default="todos",
                    help="ex.: 2024,2025 — 'todos' nao filtra nada")
    ap.add_argument("--indicadores", default="DEC,FEC,NumCon",
                    help="SigIndicador dos apurados. 'todos' traz as parcelas "
                         "desagregadas tambem (base bem maior)")
    ap.add_argument("--csv-plano", action="store_true",
                    help="CSV sem gzip, mesmo quando o arquivo for grande")
    args = ap.parse_args()

    anos = None if args.anos == "todos" else \
        [a.strip() for a in args.anos.split(",") if a.strip()]
    inds = None if args.indicadores == "todos" else \
        [i.strip() for i in args.indicadores.split(",") if i.strip()]
    print(f"fonte: {args.fonte} | anos: {args.anos} | "
          f"indicadores: {args.indicadores}\n")

    tmpdir = tempfile.mkdtemp(prefix="aneel_")
    try:
        apur, fonte_ap, meta_ap = carrega_apurados(args.fonte, anos, inds, tmpdir)
        grava(apur, "aneel_continuidade_apurados", args.csv_plano)

        lim, fonte_li, meta_li = carrega_limites(args.fonte, anos, tmpdir)
        grava(lim, "aneel_continuidade_limites", args.csv_plano)

        print("\n[3/3] dominio de indicadores ...")
        dom = ckan_fetch(RES_DOMINIO)
        dom = dom.drop(columns=[c for c in ["_id", "rank"] if c in dom])
        grava(dom, "aneel_dominio_indicadores", args.csv_plano)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    anos_ap = anos_presentes(apur, "AnoIndice")
    anos_li = anos_presentes(lim, "AnoLimiteQualidade")
    meses = sorted(pd.to_numeric(apur["NumPeriodoIndice"], errors="coerce")
                   .dropna().astype(int).unique()) if "NumPeriodoIndice" in apur else []
    ultimo = None
    if anos_ap and "NumPeriodoIndice" in apur:
        no_ano = apur[apur["AnoIndice"].astype(str).str.strip().str[:4] == anos_ap[-1]]
        m = pd.to_numeric(no_ano["NumPeriodoIndice"], errors="coerce").dropna()
        if len(m):
            ultimo = f"{anos_ap[-1]}-{int(m.max()):02d}"

    meta = {
        "baixado_em_utc": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fonte": {"apurados": fonte_ap, "limites": fonte_li, "dominio": "api"},
        "pedido": {"anos": args.anos, "indicadores": args.indicadores},
        "cobertura": {
            "anos_apurados": anos_ap,
            "anos_limites": anos_li,
            "meses_presentes": [int(x) for x in meses],
            "ultimo_mes_apurado": ultimo,
        },
        "origem": {
            "portal": "https://dadosabertos.aneel.gov.br/dataset/"
                      "indicadores-coletivos-de-continuidade-dec-e-fec",
            "recurso_apurados": meta_ap.get("nome") or RES_APURADOS,
            "recurso_apurados_modificado_em": meta_ap.get("modificado_em"),
            "recurso_limites": meta_li.get("nome") or RES_LIMITES,
            "recurso_limites_modificado_em": meta_li.get("modificado_em"),
            "geracao_apurados": str(apur["DatGeracaoConjuntoDados"].max())
            if "DatGeracaoConjuntoDados" in apur else None,
            "geracao_limites": str(lim["DatGeracaoConjuntoDados"].max())
            if "DatGeracaoConjuntoDados" in lim else None,
        },
        "linhas": {"apurados": len(apur), "limites": len(lim), "dominio": len(dom)},
        "arquivos": {f: {"bytes": os.path.getsize(os.path.join(DIR_DADOS, f)),
                         "sha256": sha256(os.path.join(DIR_DADOS, f))}
                     for f in sorted(os.listdir(DIR_DADOS))
                     if not f.startswith(".") and f != "metadados.json"},
    }
    with open(os.path.join(DIR_DADOS, "metadados.json"), "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)

    print(f"\n[ok] apurados={len(apur)} ({fonte_ap})  "
          f"limites={len(lim)} ({fonte_li})  dominio={len(dom)}")
    print(f"     anos apurados: {', '.join(anos_ap) or '-'}")
    print(f"     ultimo mes apurado: {ultimo or '-'}")

    resumo = os.environ.get("GITHUB_STEP_SUMMARY")
    if resumo:
        with open(resumo, "a", encoding="utf-8") as fh:
            fh.write(
                "## Base ANEEL atualizada\n\n"
                "| item | valor |\n|---|---|\n"
                f"| fonte | apurados: {fonte_ap} · limites: {fonte_li} |\n"
                f"| anos apurados | {', '.join(anos_ap) or '-'} |\n"
                f"| **último mês apurado** | **{ultimo or '-'}** |\n"
                f"| anos com limite | {', '.join(anos_li) or '-'} |\n"
                f"| apurados | {len(apur):,} linhas |\n"
                f"| limites | {len(lim):,} linhas |\n"
                f"| gerado na origem | {meta['origem']['geracao_apurados']} |\n"
                f"| recurso modificado em | "
                f"{meta['origem']['recurso_apurados_modificado_em']} |\n")
            if fonte_ap == "api":
                fh.write("\n> O download do arquivo-fonte falhou e a coleta caiu "
                         "para a API, que cobre menos anos. Rode de novo para "
                         "tentar o arquivo completo.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
