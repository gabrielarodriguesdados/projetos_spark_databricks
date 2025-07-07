# Databricks notebook source
# DBTITLE 1,1 -  Ingesta de datos y transformación de xlsx a csv en python
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/gabrieladev@outlook.com.br/reporte_fd.csv")
df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/gabrieladev@outlook.com.br/parametria_comercio.csv")
df3 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/gabrieladev@outlook.com.br/db_dota.csv")

# COMMAND ----------

# DBTITLE 1,2 -  Creación de vistas para normalización de datos en SQL.
df1.createOrReplaceTempView("reporte_fd_view")
df2.createOrReplaceTempView("parametria_comercio_view")
df3.createOrReplaceTempView("db_dota_view")

# COMMAND ----------

# DBTITLE 1,3 - Normalizando datos de la tabla db_dota
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW db_dota_view_transformation AS
# MAGIC SELECT *,
# MAGIC     -- 1. Estoy concatenando partes del número de la tarjeta.
# MAGIC     CONCAT(CARD_SIX_FIRST_DIGITS, 'XXXXXX', CARD_FOUR_LAST_DIGITS) AS CARD_NUMBER,
# MAGIC     
# MAGIC     -- 2. Estoy verificando la condición para GTWC_AUTHORIZATION_CODE.
# MAGIC     CASE 
# MAGIC         WHEN CAPTURE_AUTHORIZATION_CODE = '000000' AND CAPTURE_ACQUIRER = 'Cabal' THEN NULL
# MAGIC         ELSE CAPTURE_AUTHORIZATION_CODE
# MAGIC     END AS GTWC_AUTHORIZATION_CODE,
# MAGIC     
# MAGIC     -- 3. Estoy definiendo el GTWT_ACQUIRER basándome en las condiciones especificadas.
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(CAPTURE_ACQUIRER, PURCHASE_ACQUIRER, AUTH_ACQUIRER) IN ('Mastercard', 'Firstdata', 'Diners') THEN 'FD'
# MAGIC         WHEN COALESCE(CAPTURE_ACQUIRER, PURCHASE_ACQUIRER, AUTH_ACQUIRER) = 'Visa' THEN 'PRISMA'
# MAGIC         ELSE UPPER(COALESCE(CAPTURE_ACQUIRER, PURCHASE_ACQUIRER, AUTH_ACQUIRER))
# MAGIC     END AS GTWT_ACQUIRER,
# MAGIC     
# MAGIC     -- 4. Estoy determinando la marca de la tarjeta (BRAND).
# MAGIC     COALESCE(
# MAGIC         CASE 
# MAGIC             WHEN PAY_METHOD IN ('MASTER', 'MAESTRO', 'MASTERCARD', 'Master') THEN 'MASTERCARD' 
# MAGIC             ELSE PAY_METHOD 
# MAGIC         END,
# MAGIC         CASE 
# MAGIC             WHEN LEFT(TRIM(CARD_SIX_FIRST_DIGITS), 2) IN ('34', '37') THEN 'AMERICAN EXPRESS' 
# MAGIC             WHEN LEFT(TRIM(CARD_SIX_FIRST_DIGITS), 1) IN ('5', '2') THEN 'MASTERCARD' 
# MAGIC             WHEN LEFT(TRIM(CARD_SIX_FIRST_DIGITS), 1) = '4' THEN 'VISA' 
# MAGIC         END, 
# MAGIC         UPPER(PAY_METHOD), 
# MAGIC         'PENDIENTE'
# MAGIC     ) AS BRAND,
# MAGIC     
# MAGIC     -- 5. Estoy definiendo GTWT_MERCHANT_NUMBER.
# MAGIC     COALESCE(PURCHASE_MERCHANT_NUMBER, CAPTURE_MERCHANT_NUMBER, AUTH_MERCHANT_NUMBER) AS GTWT_MERCHANT_NUMBER,
# MAGIC     
# MAGIC     -- 6. Estoy añadiendo un día a MOV_CREATED_DATE y eliminando la hora.
# MAGIC     CAST(DATEADD(DAY, 1, DATE(MOV_CREATED_DATE)) AS DATE) AS MOV_CREATION_DATE
# MAGIC     
# MAGIC FROM 
# MAGIC     db_dota_view

# COMMAND ----------

# DBTITLE 1,4 - Combinando los datos de db_dota con los parámetros comerciales y filtrando ESTÁNDAR
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW db_dota_view_transformation_and_comercio AS
# MAGIC select 
# MAGIC *
# MAGIC from
# MAGIC db_dota_view_transformation dbd
# MAGIC INNER JOIN parametria_comercio_view pc on dbd.gtwt_merchant_number = pc.comercio
# MAGIC where pc.TIPO_COMERCIO = 'ESTANDAR'

# COMMAND ----------

# DBTITLE 1,5 - Agregar 2 columnas a la tabla reporte_fd
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW reporte_fd_view_transformation AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     LEFT(NUM_TAR, 6) AS LIQ_6_TARJETA,      -- Estoy extrayendo los primeros 6 números.
# MAGIC     RIGHT(NUM_TAR, 4) AS LIQ_4_TARJETA              -- Estoy extrayendo los últimos 4 números.
# MAGIC FROM 
# MAGIC     reporte_fd_view

# COMMAND ----------

# DBTITLE 1,6 - Unir los datos de las tablas db_dota con reporte_fd
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW db_dota_with_reporte_fd_view AS
# MAGIC SELECT * FROM 
# MAGIC db_dota_view_transformation_and_comercio dbd
# MAGIC INNER JOIN reporte_fd_view_transformation rfd on 
# MAGIC dbd.GTWT_MERCHANT_NUMBER = rfd.NUM_EST
# MAGIC AND dbd.CARD_SIX_FIRST_DIGITS =  rfd.LIQ_6_TARJETA
# MAGIC AND dbd.CARD_FOUR_LAST_DIGITS = rfd.LIQ_4_TARJETA 
# MAGIC AND dbd.MOV_CREATION_DATE = date_format(to_date(rfd.FORIG_COMPRA, 'M/d/yyyy'), 'yyyy-MM-dd')
# MAGIC AND dbd.MOV_AMOUNT = cast(rfd.IMPORTE as INT)

# COMMAND ----------

# DBTITLE 1,7 - Agrupar los datos y crear la suma del campo importante
# MAGIC %sql
# MAGIC -- Estoy creando una vista temporal (pivot_table) para agrupar los datos por 'PAY_COLECTOR_DOCUMENT' y sumar los valores de 'Importe'.
# MAGIC CREATE OR REPLACE TEMP VIEW pivot_table AS
# MAGIC SELECT 
# MAGIC     PAY_COLLECTOR_DOCUMENT,
# MAGIC     SUM(Importe) AS total_importe
# MAGIC FROM 
# MAGIC     db_dota_with_reporte_fd_view
# MAGIC GROUP BY 
# MAGIC     PAY_COLLECTOR_DOCUMENT

# COMMAND ----------

# DBTITLE 1,8 - Creando la visión final con los datos y las reglas aplicadas.
# MAGIC %sql
# MAGIC -- Estoy seleccionando los datos de la vista temporal y aplicando las reglas de cálculo usando una instrucción CASE.
# MAGIC SELECT 
# MAGIC     PAY_COLLECTOR_DOCUMENT,
# MAGIC     total_importe,
# MAGIC     CASE 
# MAGIC         WHEN total_importe < 20000 THEN 0
# MAGIC         WHEN total_importe BETWEEN 20001 AND 40000 THEN (total_importe - 20000) * 0.01 + (total_importe * 0.0121)
# MAGIC         WHEN total_importe > 40000 THEN (total_importe - 40000) * 0.03 + ((total_importe - 20000) * 0.01) + (total_importe * 0.0121)
# MAGIC     END AS comission
# MAGIC FROM 
# MAGIC     pivot_table
