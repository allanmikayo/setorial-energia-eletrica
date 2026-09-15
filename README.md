# Setorial — Energia Elétrica

Base bruta de continuidade do fornecimento (DEC e FEC) das distribuidoras
brasileiras, baixada do portal de dados abertos da ANEEL e versionada aqui.

Só coleta. Nada é calculado neste repositório: o que está em `data/` é o que
o portal publicou, inclusive o texto em formato brasileiro (`",07"`, `"1.234,56"`) e
os nomes com o erro de codificação da origem (`"BraganÁa"`).

## De onde vem o dado

O portal publica as mesmas informações de duas formas, e elas **não têm a mesma
cobertura**:

- **Arquivo-fonte** (`indicadores-continuidade-coletivos-2020-2029.parquet`) —
  completo, com o ano corrente. É a fonte padrão aqui.
- **API do datastore** (`datastore_search`) — o próprio metadado do portal traz
  `datastore_contains_all_records_of_source_file: false` para os apurados, e na
  prática ela para em 2025.

Por isso o padrão é `fonte: auto`: baixa o arquivo completo e, se o download
falhar (o portal derruba transferência grande de vez em quando), cai sozinho
para a API e avisa no resumo da execução que aquela rodada ficou com menos anos.

O campo `ultimo_mes_apurado` em `data/metadados.json` — e a linha em negrito no
resumo da execução — é o jeito rápido de conferir até onde a base vai.

## Como atualizar a base

1. Aba **Actions** → **Atualizar base ANEEL (DEC/FEC)** → botão **Run workflow**.
2. Os campos vêm preenchidos; é só confirmar.
3. Quando terminar, os arquivos em `data/` já estão commitados. Se nada mudou
   na origem, ele não commita e avisa no resumo da execução.

Leva uns 3 a 6 minutos. O resumo da execução mostra quantas linhas vieram e a
data de geração na origem (`DatGeracaoConjuntoDados`), que é como saber se a
ANEEL realmente publicou algo novo.

### Campos do "Run workflow"

| campo | padrão | para que serve |
|---|---|---|
| `fonte` | `auto` | `arquivo` força o arquivo completo e falha se ele não vier; `api` força a API paginada; `auto` tenta o arquivo e usa a API como reserva. |
| `anos` | `todos` | Restringe o download, ex. `2024,2025`. `todos` traz tudo que a fonte tiver — pelo arquivo, de 2020 até o ano corrente; pela API, só 2022–2025. |
| `indicadores` | `DEC,FEC,NumCon` | `NumCon` é o número de unidades consumidoras do conjunto, usado para ponderar. `todos` traz também as parcelas desagregadas (DECIP, DECXP, DECIN*...), o que multiplica a base por ~8. |
| `csv_plano` | desmarcado | Por padrão, CSV acima de 8 MB sai como `.csv.gz` para não inchar o repositório a cada atualização. Marque se precisar do `.csv` puro. |

Para ligar atualização automática, descomente o bloco `schedule` em
`.github/workflows/atualizar-base-aneel.yml`. A ANEEL publica mensalmente.

## O que fica em `data/`

| arquivo | conteúdo |
|---|---|
| `aneel_continuidade_apurados.parquet` / `.csv.gz` | DEC, FEC e NumCon mensais por conjunto de unidades consumidoras |
| `aneel_continuidade_limites.parquet` / `.csv` | Limite anual de DEC e FEC por conjunto |
| `aneel_dominio_indicadores.parquet` / `.csv` | O que cada `SigIndicador` significa |
| `metadados.json` | Fonte usada (arquivo ou API), cobertura (anos presentes, meses, **último mês apurado**), data de modificação do recurso na origem, contagem de linhas e `sha256` de cada arquivo |

### Usando a base

Direto do repositório, sem baixar nada à mão:

```python
import pandas as pd

RAW = ("https://raw.githubusercontent.com/<seu-usuario>/"
       "setorial-energia-eletrica/main/data/")

apurados = pd.read_parquet(RAW + "aneel_continuidade_apurados.parquet")
limites  = pd.read_parquet(RAW + "aneel_continuidade_limites.parquet")
```

Se o repositório for privado, `read_parquet` por URL não funciona — clone o
repositório e leia do disco.

Lembre que `VlrIndiceEnviado` e `VlrLimite` são **texto** em formato pt-BR.
Para virar número: `float(v.replace(".", "").replace(",", "."))`. Valores
menores que 1 vêm sem o zero à esquerda (`",07"`), o que essa conversão já
trata.

## Fonte

Dataset [Indicadores Coletivos de Continuidade (DEC e FEC)](https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade-dec-e-fec),
ANEEL — licença ODbL.

| recurso | id |
|---|---|
| apurados (arquivo, 2020–2029) | `d7f70fb1-725c-4748-afeb-65c6a78df550` |
| apurados (datastore/API) | `4493985c-baea-429c-9df5-3030422c71d7` |
| apurados (arquivo, 2010–2019) | `1706a88f-ecd6-4de9-99ee-ec240c317378` |
| limites | `fd69e1dd-fd66-4269-b60c-cc0b7eb221b4` |
| domínio de indicadores | `17fc99b7-e707-4ec4-9553-a43d7a41f7a6` |

Detalhes metodológicos: PRODIST Módulo 8. Os indicadores são apurados
**mensalmente**, por mês civil (item 172); trimestral e anual são a soma dos
meses civis do período (item 173.1). Os limites publicados nesta base são
**anuais**, um por conjunto/indicador/ano.

## Rodando localmente

```bash
pip install -r requirements.txt
python scripts/baixar_aneel.py --anos 2025
python scripts/baixar_aneel.py --fonte api        # força a API
python scripts/test_baixar.py     # testes, sem rede
```
