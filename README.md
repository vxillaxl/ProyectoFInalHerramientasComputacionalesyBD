# Proyecto 3. Análisis de accidentes de tránsito en zonas urbanas

Integración de accidentes de tránsito en Estados Unidos con el clima y la población estatal, para encontrar patrones y zonas críticas. El periodo va del 14 de enero de 2016 al 31 de marzo de 2023 y queda en 7.728.394 accidentes.

El dataset es [US Accidents (2016-2023)](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents), licencia CC BY-NC-SA 4.0. La población estatal sale del Census Bureau, Vintage 2023, y está en `data/NST-EST2023-ALLDATA.csv`. El CSV de accidentes no se incluye: pesa cerca de 3 GB. Se pasa con `--data-path`.

## ETL

`src/etl_accidentes.py` corre en PySpark 4.2. Extrae las columnas útiles, quita duplicados por identificador, descarta registros sin ubicación o con severidad fuera de 1 a 4, y arma las variables que la fuente no trae: franja horaria, día, clima agrupado, tipo de vía y tipo de vehículo. Responde las diez preguntas, cruza cada estado con su población y exporta un CSV por resultado. Las funciones de ventana `rank` y `row_number` dejan las cinco ciudades con más accidentes dentro de cada estado.

```powershell
python src/etl_accidentes.py --data-path RUTA\US_Accidents_March23.csv
python src/anova_severidad.py
```

Dependencias en `requirements.txt`.

## Resultados

- `resultados/01` a `10`: las diez preguntas del enunciado.
- `resultados/11_demografia_estados.csv`: accidentes, población y tasa por 100.000 habitantes.
- `resultados/12_top_ciudades_por_estado.csv`: ranking de ciudades dentro de cada estado.
- `resultados/anova_resultado.json` y las figuras `anova_franjas.png` y `anova_boxplot.png`.

Alaska y Hawái no aparecen en la fuente de accidentes. El censo sí los trae, en `modelo/dim_estado.csv`, y ningún estado con accidentes quedó sin población.

## ANOVA

La severidad (1 a 4) se compara entre madrugada, mañana, tarde y noche. La hipótesis nula es que la media es la misma en las cuatro franjas. El cálculo usa todos los registros. Shapiro-Wilk y Levene se hacen sobre una muestra, porque esas pruebas no están hechas para millones de filas.

El resultado es F(3, 7.728.390) = 1307,68, p < 0,001 y eta cuadrado = 0,000507. La diferencia es estadísticamente significativa y el efecto es insignificante: la severidad se mantiene cerca de 2,2 durante todo el día. La normalidad no se cumple, porque la severidad es una escala de cuatro niveles.

## Modelo y Power BI

`modelo/` tiene el modelo en estrella:

- `fact_diario`: accidentes por día y estado.
- `fact_hora` y `fact_clima`: tablas de apoyo, sin relación con la tabla de hechos.
- `dim_fecha`: calendario continuo. Hay que marcarla como tabla de fechas, columna `fecha`.
- `dim_estado`: nombre, región y población 2023.

Relaciones, de varios a uno:

- `fact_diario[fecha]` → `dim_fecha[fecha]`
- `fact_diario[state]` → `dim_estado[state]`

`dax/medidas.dax` tiene las medidas del enunciado: suma, promedios con `DIVIDE`, `CALCULATE`, segmentación temporal, `TOTALYTD`, `TOTALMTD`, `SAMEPERIODLASTYEAR`, `RANKX`, `VAR`, `SEARCH`, la tasa por 100.000 habitantes y la desviación de los accidentes diarios. La medida de conteo se llama `Total de accidentes` porque la columna `accidentes` ya existe y DAX no permite que compartan nombre. Sin la tabla de fechas marcada, `TOTALYTD` y `SAMEPERIODLASTYEAR` no calculan.

El archivo funcional es `Proyecto Final - Proyecto 3.pbix`. La publicación en la web queda en:

https://app.powerbi.com/view?r=eyJrIjoiNzY5YmZiOGItYTg2Yi00ZWEwLTg0YjgtODRhYWU1ZGI4MjYwIiwidCI6IjhkMzY4MzZlLTZiNzUtNGRlNi1iYWI5LTVmNGIxNzc1NDI3ZiIsImMiOjR9
