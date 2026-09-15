# Setorial — Energia Elétrica

Base bruta de continuidade do fornecimento (DEC e FEC) das distribuidoras
brasileiras, baixada da API de dados abertos da ANEEL e versionada aqui.

Só coleta. Nada é calculado neste repositório: o que está em `data/` é o que a
API devolveu, inclusive o texto em formato brasileiro (`",07"`, `"1.234,56"`) e
os nomes com o erro de codificação da origem (`"BraganÁa"`).

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
| `anos` | `todos` | Restringe o download, ex. `2024,2025`. `todos` descobre pela API os anos que o datastore tem (hoje 2022–2025). |
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
| `metadados.json` | Data do download, anos e indicadores pedidos, contagem de linhas, `sha256` de cada arquivo e a data de geração na origem |

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
| apurados | `4493985c-baea-429c-9df5-3030422c71d7` |
| limites | `fd69e1dd-fd66-4269-b60c-cc0b7eb221b4` |
| domínio de indicadores | `17fc99b7-e707-4ec4-9553-a43d7a41f7a6` |

Detalhes metodológicos: PRODIST Módulo 8.

## Rodando localmente

```bash
pip install -r requirements.txt
python scripts/baixar_aneel.py --anos 2025
python scripts/test_baixar.py     # testes, sem rede
```
