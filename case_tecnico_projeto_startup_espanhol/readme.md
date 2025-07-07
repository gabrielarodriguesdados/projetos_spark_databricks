# 📘 Documentação Técnica – Normalização e Tratamento de Dados com SQL + Databricks

## 🧾 Visão Geral

Este projeto teve como objetivo realizar a normalização e tratamento de dados financeiros e transacionais utilizando a plataforma **Databricks**, integrando múltiplas fontes de dados e aplicando regras específicas de negócio. O trabalho envolveu a leitura de arquivos CSV, criação de visões temporárias, junções entre bases, limpeza de dados e cálculo de comissões.

---

## 🔹 Etapas e Processos Realizados

### 1. **Leitura dos Arquivos de Entrada**
Os seguintes arquivos foram carregados no ambiente Databricks:

- `reporte_fd.csv`: contém registros financeiros com número da transação, valores, datas e números de cartão.
- `parametria_comercio.csv`: contém metadados sobre comércios, como tipo e identificador.
- `db_dota.csv`: base principal com movimentações detalhadas, adquirentes, bandeiras, valores e datas.

Esses arquivos foram lidos com Spark e armazenados em `DataFrames`.

---

### 2. **Criação de Views Temporárias**
Cada `DataFrame` foi convertido em uma **view temporária** para que pudesse ser manipulado usando comandos SQL dentro do Databricks.

Views criadas:
- `reporte_fd_view`
- `parametria_comercio_view`
- `db_dota_view`

---

### 3. **Transformação dos Dados da Base db_dota**

Foi criada a view `db_dota_view_transformation`, aplicando as seguintes transformações:

- **Máscara do cartão**: concatenação dos 6 primeiros e 4 últimos dígitos, mascarando os intermediários com `XXXXXX`.
- **Código de autorização**: substituição por `NULL` caso o valor seja `'000000'` e o adquirente seja `'Cabal'`.
- **Adquirente unificado**: conversão condicional para `'FD'`, `'PRISMA'` ou o valor em caixa alta.
- **Bandeira da transação**: normalização dos nomes das bandeiras com base no `PAY_METHOD` ou prefixo do número do cartão.
- **Número do comércio**: consolidação dos campos de adquirência para formar um identificador único.
- **Data de criação da movimentação**: adição de um dia à data original e remoção da hora.

---

### 4. **Filtragem por Tipo de Comércio**
Foi realizada uma junção da base transformada `db_dota_view_transformation` com a `parametria_comercio_view`, filtrando apenas os comércios do tipo `'ESTANDAR'`.

Resultado armazenado na view:
- `db_dota_view_transformation_and_comercio`

---

### 5. **Transformações na Base reporte_fd**

Na view `reporte_fd_view`, foram criadas duas novas colunas:

- `LIQ_6_TARJETA`: 6 primeiros dígitos do número do cartão
- `LIQ_4_TARJETA`: 4 últimos dígitos do número do cartão

Essas colunas foram usadas para facilitar a junção com a base `db_dota`.

---

### 6. **Junção das Bases e Cruzamento de Informações**

Criada a view `db_dota_with_reporte_fd_view`, unindo as informações de:

- `db_dota_view_transformation_and_comercio`
- `reporte_fd_view_transformation`

Critérios de junção:
- Número do comércio
- Dígitos do cartão (6 primeiros e 4 últimos)
- Data da movimentação (com 1 dia de diferença ajustado)
- Valor da transação

Essa junção permitiu consolidar os dados de transações entre os dois arquivos.

---

### 7. **Cálculo de Total por Documento Coletor**

Criada a view `pivot_table` com o agrupamento por `PAY_COLLECTOR_DOCUMENT`, somando o campo `Importe`.

Objetivo: calcular o valor total transacionado por documento coletor.

---

### 8. **Cálculo da Comissão**

Aplicadas as regras de negócio para cálculo da comissão:

- **< R$20.000**: comissão = R$0
- **R$20.001 a R$40.000**: comissão = 1% sobre o excedente + 1.21% do total
- **> R$40.000**: comissão = 
  - 3% sobre o que exceder R$40.000
  - 1% sobre os valores entre R$20.001 e R$40.000
  - 1.21% sobre o total

Essa lógica foi aplicada diretamente em SQL com `CASE WHEN`.

---

## 📤 Saídas Geradas

- Base final com comissões por documento
- Arquivo CSV com os dados já tratados
- Planilha Excel com as transformações e regras aplicadas

---

## 🛠 Tecnologias Utilizadas

- 📍 **Apache Spark**
- 📍 **SQL + Python (PySpark)**
- 📍 **Databricks**
- 📍 **Arquivos CSV e Excel**

---

## 🔗 Notebook na Nuvem

[Acesse o notebook Databricks com código completo e execução](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1186711081889598/3474049659358438/2401084538103174/latest.html)

---
