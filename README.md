# Mantenimiento Predictivo Modular

Repositorio modular para refactorizar un notebook monolítico de mantenimiento predictivo en una pipeline reproducible, reutilizable y lista para iterar nuevas features.

## Objetivo

El proyecto conserva la funcionalidad del notebook original que:

- carga JSON de preventivos y correctivos,
- limpia registros eliminados,
- consolida revisiones ejecutadas,
- agrupa correctivos por evento técnico,
- construye variables históricas y temporales,
- entrena clasificadores XGBoost para horizontes de 7, 5 y 3 días,
- evalúa con reportes de clasificación, matrices de confusión y curvas Precision-Recall.

La refactorización además deja preparada la base para anonimización de `placa_patente` y experimentación iterativa de nuevas variables.

## Estructura

```text
repo_mantenimiento_predictivo/
├── data/
│   ├── raw/
│   │   ├── preventivos.json
│   │   └── correctivos.json
│   └── processed/
│       ├── base.parquet
│       ├── eventos.parquet
│       └── features.parquet
├── models/
│   ├── xgb_7d.pkl
│   ├── xgb_5d.pkl
│   └── xgb_3d.pkl
├── notebooks/
│   ├── 01_carga_y_limpieza.ipynb
│   ├── 02_creacion_eventos.ipynb
│   ├── 03_feature_engineering.ipynb
│   ├── 04_analisis_exploratorio.ipynb
│   ├── 05_modelado_xgboost.ipynb
│   └── 06_experimentos_iterativos.ipynb
├── outputs/
│   ├── metrics/
│   └── plots/
├── src/
│   ├── anonymization.py
│   ├── data_loader.py
│   ├── evaluation.py
│   ├── feature_engineering.py
│   ├── modeling.py
│   └── preprocessing.py
├── README.md
└── requirements.txt
```

## Orden de ejecución

Ejecuta los notebooks en este orden:

1. `notebooks/01_carga_y_limpieza.ipynb`
2. `notebooks/02_creacion_eventos.ipynb`
3. `notebooks/03_feature_engineering.ipynb`
4. `notebooks/04_analisis_exploratorio.ipynb`
5. `notebooks/05_modelado_xgboost.ipynb`
6. `notebooks/06_experimentos_iterativos.ipynb`

Cada notebook persiste artefactos intermedios en `data/processed/`, gráficos en `outputs/plots/`, métricas en `outputs/metrics/` y modelos en `models/`.

## Cómo correr los notebooks

1. Instala dependencias:

```bash
pip install -r requirements.txt
```

2. Abre Jupyter en la raíz del repositorio:

```bash
jupyter lab
```

3. Ejecuta los notebooks en orden para regenerar parquets, métricas y modelos.

## Cómo agregar nuevas features

- Añade la transformación reutilizable en `src/feature_engineering.py`.
- Regenera `data/processed/features.parquet` desde `03_feature_engineering.ipynb`.
- Declara la nueva combinación de columnas en `05_modelado_xgboost.ipynb` o `06_experimentos_iterativos.ipynb`.
- Reentrena y compara métricas guardadas en `outputs/metrics/`.

## Cómo anonimizar datos

El módulo `src/anonymization.py` provee:

- `hash_bus_identifier()`: aplica SHA256 a `placa_patente`.
- `anonymize_dataset()`: reemplaza identificadores manteniendo intacta la estructura temporal.

Ejemplo:

```python
from pathlib import Path
import pandas as pd

from src.anonymization import anonymize_dataset

features = pd.read_parquet(Path("data/processed/features.parquet"))
features_anon = anonymize_dataset(features, salt="mi_semilla_privada")
```

## Cómo reentrenar modelos

- Reejecuta `05_modelado_xgboost.ipynb` para regenerar `xgb_7d.pkl`, `xgb_5d.pkl` y `xgb_3d.pkl`.
- Si cambias features o parámetros, registra el experimento en `06_experimentos_iterativos.ipynb`.
- Las métricas se guardan por horizonte y configuración de SMOTE en `outputs/metrics/`.

## Nota de equivalencia

La pipeline mantiene la lógica del notebook original, incluyendo:

- split estratificado con `random_state=42`,
- parámetros base de XGBoost,
- uso opcional de SMOTE,
- cálculo de `scale_pos_weight`,
- thresholds de evaluación `0.3`, `0.4`, `0.5` y `0.6`,
- features históricas y temporales usadas originalmente.

La base modular separa la etapa de eventos técnicos únicos para permitir futuras iteraciones sin volver a depender de un notebook monolítico.
