# ANÁLISIS TÉCNICO: READINESS PARA PILOTO 1 — SHADOW MODE

**Auditoría:** Repositorio `repo_mantenimiento_predictivo`
**Fecha:** 09 Mayo 2026
**Operador:** VOY
**Versión:** Post-correcciones (pipeline ejecutable + scripts de inferencia)

---

## ÍNDICE

1. [Resumen Ejecutivo](#1-resumen-ejecutivo)
2. [Pipeline Completo](#2-pipeline-completo)
3. [Dataset VOY — Análisis Real](#3-dataset-voy--análisis-real)
4. [Modelo XGBoost — Métricas Reales](#4-modelo-xgboost--métricas-reales)
5. [Scripts Creados](#5-scripts-creados)
6. [Shadow Mode — Estado Actual](#6-shadow-mode--estado-actual)
7. [Clasificación de Fallas](#7-clasificación-de-fallas)
8. [Selección de Buses](#8-selección-de-buses)
9. [Riesgos Metodológicos](#9-riesgos-metodológicos)
10. [Próximos Pasos](#10-próximos-pasos)

---

## 1. RESUMEN EJECUTIVO

| Dimensión | Estado anterior | Estado actual |
|---|---|---|
| Pipeline ejecutable | ⚠️ Sin datos | ✅ Pipeline completo ejecutado |
| Leakage temporal | ❌ Split aleatorio | ✅ Split temporal (cutoff 2025-11-15) |
| Inferencia batch | ❌ No existía | ✅ `scripts/batch_inference.py` |
| Consulta por bus | ❌ No existía | ✅ `scripts/consultar_bus.py` |
| Pipeline runner | ❌ Solo notebooks | ✅ `scripts/run_pipeline.py` |
| Clasificación de fallas | ❌ No existía | ✅ Severidad LOW/MEDIUM/HIGH |
| Datos raw | ❌ Ausentes | ✅ VOY + REDBUS presentes |

**Estado actual: NO completamente listo para Shadow Mode real**, pero la infraestructura base está operativa.

---

## 2. PIPELINE COMPLETO

### 2.1 Archivos creados/corregidos

| Archivo | Propósito |
|---|---|
| `data/raw/preventivos.json` → symlink a `voy_preventivos.json` | Carga de datos VOY |
| `data/raw/correctivos.json` → symlink a `voy_correctivos.json` | Carga de datos VOY |
| `scripts/run_pipeline.py` | Pipeline completo ETL → training → inference |
| `scripts/batch_inference.py` | Generación de predicciones batch |
| `scripts/consultar_bus.py` | Consulta CLI de predicciones por bus |
| `data/predictions/predictions.parquet` | Output: 36,621 predicciones (727 buses × 3 horizontes) |

### 2.2 Pipeline ejecutado exitosamente

```
STEP 01: Load and clean data → 15,026 registros, 735 buses, 2024-11-05 → 2025-12-31
STEP 02: Create technical events → 12,207 eventos técnicos únicos
STEP 03: Feature engineering → 126 columnas, targets 3/5/7/10/14/30 días
STEP 04: Train XGBoost → 3 modelos con split temporal (cutoff: 2025-11-15)
STEP 05: Batch inference → 36,621 predicciones guardadas
```

---

## 3. DATASET VOY — ANÁLISIS REAL

### 3.1 Estadísticas

| Métrica | Valor |
|---|---|
| Preventivos raw | 636 (526 ejecutados) |
| Correctivos raw | 14,500 (todos ejecutados) |
| Buses únicos | 727 (correctivos), 735 (total) |
| Rango temporal correctivos | 2025-05-20 → 2025-12-31 (~7 meses) |
| Rango temporal total | 2024-11-05 → 2025-12-31 (~14 meses) |
| Eventos técnicos | 12,207 |
| Promedio días entre correctivos | 7.9 días |
| Mediana días entre correctivos | 3.5 días |

### 3.2 Problema crítico: taxonomía de fallas

| Causa | % Eventos |
|---|---|
| `VARIOS` (genérico) | 76.7% |
| `CORRECTIVO` (genérico) | 19.8% |
| `MANTENIMIENTO PREVENTIVO` | 3.5% |
| `sistema_componente` = `VARIOS` | **100%** |

**Conclusión:** Los datos crudos NO tienen taxonomía de fallas. No existe columna de severidad, tipo de falla, código de falla, ni sistema afectado. La única señal semántica proviene de:
- `causa_sistema_reconstruida` (ingeniería de texto): OTROS 79.3%, CARROCERIA 7.6%, FRENOS 4.1%, MOTOR 3.7%, ELECTRICO 2.4%
- Keywords técnicas presentes en solo 24.9% de eventos
- `repuestos_count` como proxy de complejidad

### 3.3 Clasificación por severidad (implementada)

| Severidad | % Eventos | Criterio |
|---|---|---|
| LOW | 23.3% | Sin repuestos, duración < 2h, sin keywords |
| MEDIUM | 53.2% | Con repuestos o duración > 2h |
| HIGH | 23.4% | Repuestos + duración > 4h |

---

## 4. MODELO XGBoost — MÉTRICAS REALES

### 4.1 Split temporal (corrección del leakage)

Se implementó split temporal con cutoff `2025-11-15`:
- **Train:** 6,783 eventos (May → Nov 2025)
- **Test:** 5,424 eventos (Nov → Dic 2025)

### 4.2 Resultados (threshold 0.5, sin SMOTE)

| Horizonte | Accuracy | Precision | Recall | F1-score | CM (TN, FP, FN, TP) |
|---|---|---|---|---|---|
| **7 días** | **0.611** | **0.838** | **0.562** | **0.673** | [[1148, 418], [1691, 2167]] |
| 5 días | 0.567 | 0.763 | 0.453 | 0.568 | [[1530, 480], [1869, 1545]] |
| 3 días | 0.580 | 0.670 | 0.345 | 0.455 | [[2194, 469], [1809, 952]] |

**Ningún horizonte alcanza Accuracy ≥ 0.75.** La métrica reportada originalmente (0.76 para 7d) era artificial por el leakage del split aleatorio.

### 4.3 Interpretación

- **Precisión alta (0.84 en 7d):** Cuando el modelo dice que habrá un evento correctivo, acierta el 84% de las veces.
- **Recall bajo (0.56 en 7d):** El modelo detecta solo el 56% de los eventos correctivos reales.
- **El modelo es conservador:** Prefiere no alertar antes que alertar incorrectamente.
- **Clase mayoritaria es la positiva** (71%): Es inusual para predicción de fallas, pero refleja la alta frecuencia de correctivos en la flota.

---

## 5. SCRIPTS CREADOS

### 5.1 `scripts/run_pipeline.py`

Pipeline completo. Un solo comando:

```bash
python scripts/run_pipeline.py
```

### 5.2 `scripts/batch_inference.py`

Genera predicciones usando modelos entrenados. Independiente del pipeline:

```bash
python scripts/batch_inference.py
```

Output: `data/predictions/predictions.parquet` con esquema:
- `placa_patente` — ID del bus
- `fecha_evento` — timestamp del evento base
- `horizon_days` — 3, 5, o 7 días
- `probability` — probabilidad clase 1 (0-1)
- `alert` — booleano (probability >= 0.5)
- `severity` — LOW / MEDIUM / HIGH

### 5.3 `scripts/consultar_bus.py`

Interfaz CLI para consultar predicciones:

```bash
# Listar todos los buses ordenados por alertas
python scripts/consultar_bus.py

# Consultar un bus específico
python scripts/consultar_bus.py PFVL15

# Solo alertas activas
python scripts/consultar_bus.py PFVL15 --alerts

# Filtrar por horizonte
python scripts/consultar_bus.py PFVL15 --horizon 7

# Top 10 buses con más riesgo
python scripts/consultar_bus.py --top 10
```

---

## 6. SHADOW MODE — ESTADO ACTUAL

### 6.1 Lo que funciona

| Componente | Estado | Detalle |
|---|---|---|
| Pipeline ETL batch | ✅ | `run_pipeline.py` completo |
| Modelos entrenados | ✅ | xgb_3d/5d/7d.pkl con split temporal |
| Predicciones generadas | ✅ | 36,621 predicciones en parquet |
| Consulta por bus | ✅ | CLI con filtros |
| Severidad de eventos | ✅ | LOW/MEDIUM/HIGH clasificado |

### 6.2 Lo que falta para Shadow Mode real

| Componente | Estado | Prioridad |
|---|---|---|
| Ingesta automatizada de nuevos eventos | ❌ | Alta |
| Programación diaria (cron/Airflow) | ❌ | Alta |
| Log de predicciones con timestamp real | ⚠️ Parcial | Media |
| Comparación contra eventos reales | ❌ | Alta |
| Reentrenamiento automático | ❌ | Media |
| Monitoreo de drift | ❌ | Media |
| Dashboard/UI | ❌ | Baja |

---

## 7. CLASIFICACIÓN DE FALLAS

### 7.1 Método implementado

Se implementó clasificación de severidad en `batch_inference.py` basada en:

```python
def classify_severity(row):
    has_parts = row.repuestos_count_evento > 0
    duration = row.duracion_ot_horas_prom_evento or 0
    keywords = row.num_keywords_tecnicos_evento or 0
    if not has_parts and duration < 2 and keywords == 0:
        return "LOW"      # Evento trivial (ej. inspección)
    elif has_parts and duration > 4:
        return "HIGH"     # Evento mayor (ej. reconstrucción)
    return "MEDIUM"       # Evento intermedio
```

### 7.2 Distribución de severidad

| Severidad | % Eventos | Interpretación |
|---|---|---|
| **LOW** | 23.3% | Sin repuestos, corta duración. Probablemente ajustes o inspecciones. No representan fallas reales. |
| **MEDIUM** | 53.2% | Con repuestos o duración moderada. La mayoría de los correctivos. |
| **HIGH** | 23.4% | Múltiples repuestos y larga duración. Fallas significativas. |

### 7.3 Implicación para Pilot 1

El modelo actual predice **cualquier evento correctivo**, no solo fallas significativas. Esto diluye la métrica: una alerta puede ser "correcta" (habrá un correctivo) pero para un evento trivial (LOW). Para Shadow Mode, se recomienda:
- Filtrar evaluación solo con eventos MEDIUM+HIGH como "fallas reales"
- O crear un modelo específico para predecir eventos HIGH

---

## 8. SELECCIÓN DE BUSES

### 8.1 Buses con más eventos (candidatos para Pilot 1)

| Bus | Eventos | Alertas 7d | Alert% | Riesgo Promedio |
|---|---|---|---|---|
| PFVL15 | 64 | 59 | 92.2% | 84.2% |
| PFVK90 | 60 | 54 | 90.0% | 78.4% |
| PDZH97 | 54 | 51 | 94.4% | 90.9% |
| PFTF88 | 54 | 50 | 92.6% | 87.1% |
| PFYH17 | 51 | 49 | 96.1% | 90.2% |

### 8.2 Restricciones para Pilot 1

| Requisito | Estado |
|---|---|
| 12+ meses de datos | ❌ Solo ~7 meses de correctivos (May-Dic 2025) |
| 2 diésel + 1 eléctrico | ❌ No hay columna de tipo de combustible |
| Telemetría estable | ❌ No hay datos de sensores continuos |
| Mínimo 3 buses | ✅ 727 buses disponibles |

### 8.3 Recomendación

Usar los 3 buses con más eventos históricos (PFVL15, PFVK90, PDZH97) como proxy, documentando la limitación de cobertura temporal.

---

## 9. RIESGOS METODOLÓGICOS

| Riesgo | Severidad | Estado |
|---|---|---|
| **Split temporal** | ❌ Crítico → ✅ Corregido | Ahora usa cutoff 2025-11-15 |
| **Taxonomía de fallas** | 🔴 ALTO | 76.7% eventos son "VARIOS" |
| **Solo 7 meses correctivos** | 🔴 ALTO | No cumple requisito de 12 meses |
| **Clase desbalanceada al revés** | 🟡 MEDIO | Clase positiva es mayoritaria (71%) |
| **Sin tipo de combustible** | 🔴 ALTO | No se pueden identificar diésel vs eléctrico |
| **Sin telemetría continua** | 🔴 ALTO | Datos solo de órdenes de trabajo |
| **Alto número de falsos negativos** | 🟡 MEDIO | Recall 0.56 en 7d |
| **Sin diferenciación de severidad en target** | 🟡 MEDIO | Target = "cualquier correctivo" |

---

## 10. PRÓXIMOS PASOS

### Inmediatos (días 1-7)
1. ✅ Pipeline ejecutable — **LISTO**
2. ✅ Inferencia + consulta — **LISTO**
3. ⬜ Comparación contra eventos reales — implementar `scripts/evaluate_shadow.py`
4. ⬜ Evaluar accuracy SOLO en eventos MEDIUM+HIGH

### Corto plazo (semanas 2-4)
5. ⬜ Obtener datos de 12+ meses (REDBUS tiene desde 2022)
6. ⬜ Solicitar a VOY el mapeo bus → tipo de combustible
7. ⬜ Implementar programación diaria de inferencia (cron)
8. ⬜ Crear log de predicciones con `prediction_timestamp`

### Medio plazo (meses 2-3)
9. ⬜ Entrenar modelo específico para severidad HIGH
10. ⬜ Implementar detección de drift
11. ⬜ Dashboard básico (Streamlit o similar)
12. ⬜ Reentrenamiento automático periódico

---

*Documento generado el 09 Mayo 2026 — Versión post-correcciones.*
