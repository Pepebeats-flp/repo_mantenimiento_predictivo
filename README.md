# Mantenimiento Predictivo — Flota

Dashboard de predicción de fallas con XGBoost, entrenado con datos históricos de mantenimiento.

## Ramas

| Rama | Contenido | Uso |
|------|-----------|-----|
| `main` | Código completo: pipeline, experimentos, modelos, notebooks | Desarrollo local |
| `dashboard` | Solo lo necesario para el dashboard: `app.py`, `src/`, datos, reporte | Deploy en producción |

## Dashboard — Rama `dashboard`

### Archivos incluidos

```
app.py                         # Streamlit dashboard
requirements.txt               # Dependencias mínimas
src/__init__.py
src/data_loader.py
src/preprocessing.py
data/processed/base.parquet    # Datos base (88 MB)
data/predictions/predictions_voy_redbus.parquet   # Predicciones (7.3 MB)
outputs/piloto1_report.json    # Reporte shadow evaluation (59 MB)
```

### Archivos NO incluidos (están solo en `main`)

Modelos entrenados, features, experimentos, scripts de pipeline, notebooks, `archived/`.

## Cómo actualizar el dashboard

Después de correr el pipeline en `main` (nuevas predicciones, nuevo reporte):

```bash
# 1. Desde main, pasate a dashboard
git checkout dashboard

# 2. Trae solo los archivos actualizados desde main
git checkout main -- app.py requirements.txt src/preprocessing.py src/data_loader.py
git checkout main -- data/predictions/predictions_voy_redbus.parquet
git checkout main -- outputs/piloto1_report.json

# 3. Commit y push a dashboard
git commit -m "sync: actualizar predicciones y reporte"
git push origin dashboard

# 4. Volvé a main para seguir trabajando
git checkout main
```

> Si cambia `src/__init__.py` o `data/processed/base.parquet`, también traerlos:
> ```bash
> git checkout main -- src/__init__.py data/processed/base.parquet
> ```

## Cómo sincronizar cambios de código

Si modificaste `app.py`, `requirements.txt` o archivos de `src/` en ambas ramas y hay conflictos:

```bash
# Opción A: sobrescribir dashboard con main (seguro si main es más reciente)
git checkout dashboard
git checkout main -- app.py requirements.txt src/
git commit -m "sync: actualizar código desde main"
git push origin dashboard
git checkout main

# Opción B: merge (si hay cambios específicos en dashboard que mantener)
git checkout dashboard
git merge main --no-commit
# Resolver conflictos manualmente
git commit -m "merge: integrar cambios de main"
git push origin dashboard
git checkout main
```

## Configurar deploy

En la plataforma de hosting (Streamlit Community Cloud, etc.):

- **Branch**: `dashboard`
- **Main file**: `app.py`
- **Python version**: 3.14+

## Pipeline completo (rama `main`)

```bash
python3 scripts/run_pipeline.py --local-json          # ETL → Train → Infer
python3 scripts/evaluate_shadow.py                    # Generar reporte
# Luego sync a dashboard (ver pasos arriba)
```

Ver `AGENTS.md` para más detalles sobre experimentos y evaluación.
