# Processamento Diário de Vendas – Conferência de Caixa

Read this project description in English: [README_EN.md](README_EN.md)

Script em Python desenvolvido para apoiar a **conferência diária de caixa dos restaurantes do Grupo NORI**, automatizando a análise de relatórios de vendas exportados do sistema da Cielo. O projeto resolve uma limitação operacional do sistema, que não permite somar e classificar os valores de forma clara por **forma de pagamento** e **canal de venda**.

O script realiza a limpeza, padronização e agregação dos dados, gerando um **resumo diário confiável** que facilita a validação financeira, reduz erros manuais e otimiza o fechamento de caixa.

## Contexto do problema
Os relatórios CSV gerados pela Cielo apresentam:
- cabeçalhos inconsistentes
- valores em formato não numérico
- ausência de consolidação por Pix, Crédito e Débito
- dificuldade para conferência rápida dos totais diários

Este script foi criado para transformar esses dados brutos em uma tabela analítica clara e pronta para uso administrativo.

## Funcionalidades
- Leitura dinâmica de arquivos CSV com cabeçalho variável
- Limpeza e conversão de datas e valores monetários (padrão brasileiro)
- Filtragem por estabelecimento
- Classificação automática das vendas por forma de pagamento (Pix, Crédito, Débito)
- Padronização dos canais de venda (Máquina / TEF)
- Agregação diária por data e canal
- Cálculo automático de totais
- Geração de resumo diário em CSV
- Organização automática dos arquivos já processados

## Tecnologias
- Python
- Pandas
- Pathlib

## Como usar
1. Coloque os arquivos CSV exportados da Cielo na pasta `entrada_diaria/`
2. Execute o script:
   ```bash
   python processamento_vendas.py
