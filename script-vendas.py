import pandas as pd
from pathlib import Path
import shutil

# CONFIGURAÇÕES
BASE = Path(__file__).parent   # Sempre aponta para a pasta do script

PASTA_ENTRADA = BASE / "entrada_diaria"
PASTA_PROCESSADOS = BASE / "processados"

ESTABELECIMENTO_ALVO = "xxxxxxxxx"
SEPARADOR = ";"

PASTA_ENTRADA.mkdir(exist_ok=True)
PASTA_PROCESSADOS.mkdir(exist_ok=True)

# LISTA DE ARQUIVOS
arquivos = list(PASTA_ENTRADA.glob("*.csv"))

if not arquivos:
    print("Nenhum arquivo CSV encontrado em entrada_diaria/")
    exit()

for arquivo in arquivos:
    print(f"Processando: {arquivo.name}")

    # Localiza início da tabela
    with open(arquivo, "r", encoding="latin-1", errors="ignore") as f:
        linhas = f.readlines()

    inicio_tabela = None
    for i, linha in enumerate(linhas):
        if linha.startswith("Data da venda;"):
            inicio_tabela = i
            break

    if inicio_tabela is None:
        print("Não foi possível localizar o cabeçalho da tabela.")
        continue

    # Lê tabela 
    df = pd.read_csv(
        arquivo,
        sep=SEPARADOR,
        encoding="latin-1",
        skiprows=inicio_tabela
    )

    # Seleciona colunas necessárias
    colunas = [
        "Data da venda",
        "Estabelecimento",
        "Forma de pagamento",
        "Valor bruto",
        "Canal da venda"
    ]

    df = df[colunas].copy()

    # Filtra estabelecimento
    df["Estabelecimento"] = df["Estabelecimento"].astype(str)
    df = df[df["Estabelecimento"] == ESTABELECIMENTO_ALVO]

    if df.empty:
        print("Nenhuma venda encontrada para o estabelecimento informado.")
        continue

    # Converte data
    df["Data da venda"] = pd.to_datetime(df["Data da venda"], dayfirst=True)

    # Converte valores para número
    df["Valor bruto"] = (
        df["Valor bruto"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

    # Mantém apenas Máquina e TEF
    df = df[df["Canal da venda"].isin(["Máquina", "Cielo LIO", "TEF"])]
    df["Canal da venda"] = df["Canal da venda"].replace({"Cielo LIO": "TEF"})

    # Classificação de forma de pagamento
    def classificar(pag):
        pag = pag.lower()
        if "pix" in pag:
            return "Pix"
        if "crédito" in pag:
            return "Crédito"
        if "débito" in pag:
            return "Débito"
        return "Outros"

    df["Categoria"] = df["Forma de pagamento"].apply(classificar)

    # Cria colunas Pix, Crédito, Débito
    tabela = df.pivot_table(
        index=["Data da venda", "Canal da venda"],
        columns="Categoria",
        values="Valor bruto",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    # Garante colunas mesmo se vazias
    for col in ["Pix", "Crédito", "Débito"]:
        if col not in tabela:
            tabela[col] = 0.0

    # Cria coluna TOTAL
    tabela["Total"] = tabela["Pix"] + tabela["Crédito"] + tabela["Débito"]

    # Formatação BR (2 casas decimais, vírgula)
    tabela_formatada = tabela.copy()
    for col in ["Pix", "Crédito", "Débito", "Total"]:
        tabela_formatada[col] = tabela_formatada[col].map(lambda x: f"{x:.2f}".replace(".", ","))

    # Salva CSV final no diretório do projeto
    caminho_resumo = BASE / "resumo_diario.csv"

    tabela_formatada.to_csv(
        caminho_resumo,
        sep=";",
        index=False,
        encoding="latin-1"
    )

    print(f"Resumo salvo em: {caminho_resumo}")

    # Move arquivo processado
    shutil.move(str(arquivo), PASTA_PROCESSADOS / arquivo.name)
    print("Arquivo movido para processados/\n")

print("Processamento concluído!")
