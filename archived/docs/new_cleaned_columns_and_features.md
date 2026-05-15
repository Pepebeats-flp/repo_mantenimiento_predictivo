# New Cleaned Columns and Features

## Raw JSON structure findings

### `data/raw/preventivos.json`

Top-level fields found:

- `placa_patente`
- `tipo_servicio`
- `pauta_proyectada`
- `sistema_componente`
- `causa_origen`
- `km_proyectado`
- `frecuencia`
- `tolerancia`
- `fecha_proyectada`
- `fecha_proyectada_timestamp`
- `user_uid`
- `user_name`
- `unidad_negocio`
- `fecha_creacion_timestamp`
- `fecha_creacion`
- `uuid`
- `timestamp`
- `estado`
- `ot_cierre_hora`
- `taller_planta`
- `fecha_actualizacion_timestamp`
- `insumos`
- `repuestos`
- `ot_apertura_hora`
- `ot_cierre_fecha`
- `fecha_actualizacion`
- `ot_apertura_fecha`
- `ot_numero`
- `ot_cierre_timestamp`
- `hora_actualizacion`
- `ot_apertura_timestamp`
- `pauta_ejecutada`
- `km_ejecucion`
- `resultado`
- `km_diferencia`
- `uuid_gestion`
- `user_uid_actualizacion`
- `user_name_actualizacion`
- `observacion`

Nested paths found:

- `$.repuestos` as list, but empty in observed preventivos.
- `$.insumos` as list, but empty in observed preventivos.
- `$.uuid_gestion[]` as list of management ids.

### `data/raw/correctivos.json`

Top-level fields found:

- `placa_patente`
- `tipo_servicio`
- `sistema_componente`
- `causa_origen`
- `ot_numero`
- `ot_apertura_fecha`
- `ot_apertura_hora`
- `ot_apertura_timestamp`
- `ot_cierre_fecha`
- `ot_cierre_hora`
- `ot_cierre_timestamp`
- `taller_planta`
- `km_ejecucion`
- `pauta_ejecutada`
- `observacion`
- `repuestos`
- `insumos`
- `user_uid`
- `user_name`
- `unidad_negocio`
- `fecha_creacion_timestamp`
- `fecha_creacion`
- `uuid`
- `timestamp`
- `estado`
- `hora_creacion`
- `uuid_gestion`

Nested paths found:

- `$.repuestos[].repuesto_codigo`
- `$.repuestos[].repuesto_cantidad`
- `$.repuestos[].repuesto_descripcion`
- `$.repuestos[].repuesto_marca`
- `$.repuestos[].repuesto_tipo`
- `$.uuid_gestion[]`
- `$.insumos` as list, but empty in observed correctivos.

### Requested-but-absent fields

The following candidate fields were explicitly searched and were not present in the observed raw JSON payloads:

- `descripcion`
- `sistema` separate from `sistema_componente`
- `subsistema`
- `tipo_falla`
- `codigo_falla`
- `severidad`
- `componente`
- `ubicacion`
- `tecnico` separate from `user_name`
- `clasificacion`
- `motivo`
- `resultado_revision`

## High-priority unused or partially used raw information

- `repuestos[]` was loaded as a raw list but never flattened into modeling-ready columns.
- `uuid_gestion[]` existed as a list but was not converted into counts or event-level signals.
- `insumos[]` was preserved raw but not profiled; in the observed data it is empty, so the new pipeline keeps safe zero-like summaries.
- Additional timestamps were available but not transformed into duration/lag columns:
  - `fecha_creacion_timestamp`
  - `fecha_proyectada_timestamp`
  - `ot_cierre_timestamp`
- `pauta_ejecutada` and `pauta_proyectada` existed as raw strings but were not normalized into compact families/programs.
- `taller_planta`, `user_name`, `unidad_negocio`, `causa_origen` and `sistema_componente` were present but not normalized or grouped.

## New cleaned columns

| Column | Source JSON path | Cleaning method | Feature transformations | Predictive hypothesis |
|---|---|---|---|---|
| `uuid_gestion_count` | `$.uuid_gestion[]` | Safe list length | Event sum and rolling counts | More management links may indicate complex failures and higher recurrence risk |
| `uuid_gestion_unique_count` | `$.uuid_gestion[]` | Unique id count | Event-level unique count | Repeated vs diverse management ids may separate routine vs complex work |
| `uuid_gestion_texto` | `$.uuid_gestion[]` | Pipe-joined normalized ids | Event-level unique aggregation | Preserves nested ids without dropping raw alignment |
| `insumos_count` | `$.insumos[]` | Safe list length | Event sum | Keeps schema stable if future JSON starts populating this list |
| `insumos_unique_count` | `$.insumos[]` | Unique count | Event-level unique aggregation | Same as above |
| `insumos_texto` | `$.insumos[]` | Pipe-joined normalized text | Event-level unique aggregation | Same as above |
| `repuestos_count` | `$.repuestos[]` | Count nested dictionaries | Event sum and rolling counts | More parts used in the current repair can signal severity/complexity |
| `repuestos_cantidad_total` | `$.repuestos[].repuesto_cantidad` | Numeric coercion and sum | Event sum and rolling counts | Larger consumed quantity may correlate with cascading failures |
| `repuestos_codigos_unicos` | `$.repuestos[].repuesto_codigo` | Unique normalized code count | Event unique count | Diverse parts can indicate broader system instability |
| `repuestos_descripciones_unicas` | `$.repuestos[].repuesto_descripcion` | Unique normalized description count | Event unique count | Same rationale as part-code diversity |
| `repuestos_original_count` | `$.repuestos[].repuesto_tipo` | Count of normalized `ORIGINAL` | Available for future use | OEM usage may proxy repair type and severity |
| `repuestos_marca_count` | `$.repuestos[].repuesto_marca` | Count non-empty normalized brands | Available for future use | Supplier/brand richness may proxy repair complexity |
| `repuestos_codigo_texto` | `$.repuestos[].repuesto_codigo` | Pipe-joined normalized text | Event-level unique aggregation | Preserves code-level evidence for downstream rules |
| `repuestos_descripcion_texto` | `$.repuestos[].repuesto_descripcion` | Pipe-joined normalized text | Keyword extraction and event text aggregation | Part descriptions contain concrete technical signals missing elsewhere |
| `repuestos_tipo_texto` | `$.repuestos[].repuesto_tipo` | Pipe-joined normalized text | Available for future use | Keeps type metadata visible for analysis |
| `repuestos_marca_texto` | `$.repuestos[].repuesto_marca` | Pipe-joined normalized text | Available for future use | Keeps supplier metadata visible for analysis |
| `fecha_creacion_timestamp_dt` | `$.fecha_creacion_timestamp` | Unix timestamp to datetime | Quality checks and lag features | Better chronology validation |
| `fecha_actualizacion_timestamp_dt` | `$.fecha_actualizacion_timestamp` | Unix timestamp to datetime | Quality checks | Better chronology validation |
| `fecha_proyectada_timestamp_dt` | `$.fecha_proyectada_timestamp` | Unix timestamp to datetime | Delay features | Projected-vs-executed delay may relate to later corrective demand |
| `ot_apertura_timestamp_dt` | `$.ot_apertura_timestamp` | Unix timestamp to datetime | Quality checks | More explicit event chronology |
| `ot_cierre_timestamp_dt` | `$.ot_cierre_timestamp` | Unix timestamp to datetime | Duration features | Repair duration can proxy severity |
| `duracion_ot_horas` | `$.ot_cierre_timestamp - $.ot_apertura_timestamp` | Numeric delta in hours | Event averages/max and rolling sums | Longer work orders can indicate harder-to-fix cases |
| `horas_desde_creacion_hasta_apertura` | `$.ot_apertura_timestamp - $.fecha_creacion_timestamp` | Numeric delta in hours | Event averages | Dispatch/creation lag can signal operational stress |
| `dias_desfase_proyectado_vs_apertura` | `$.ot_apertura_timestamp - $.fecha_proyectada_timestamp` | Numeric delta in days | Stored in base | Preventive delay may be associated with future corrective burden |
| `km_desviacion_relativa` | `$.km_diferencia / $.frecuencia` | Numeric ratio with coercion | Event averages | Relative mileage deviation can proxy maintenance discipline |
| `km_ratio_ejecucion_vs_proyectado` | `$.km_ejecucion / $.km_proyectado` | Numeric ratio with coercion | Stored in base | Deviation from planned mileage may capture schedule drift |
| `observacion_clean` | `$.observacion` | Uppercase, trim, accent removal | Stored in base | Ready if future data starts using this text field |
| `repuestos_descripcion_texto_clean` | `$.repuestos[].repuesto_descripcion` | Uppercase, trim, accent removal | Keyword extraction | Converts part text into stable lexical signals |
| `causa_origen_norm` | `$.causa_origen` | Uppercase, trim, accent removal | Event-level carry and cause features | Stable category for recurrence analysis |
| `causa_origen_grouped` | `$.causa_origen` | Top-K grouping with `OTHER` | Rolling cause counts and diversity | Reduces sparse categories without losing dominant causes |
| `sistema_componente_norm` | `$.sistema_componente` | Uppercase, trim, accent removal | Event-level carry | Stable category for system counts |
| `sistema_componente_grouped` | `$.sistema_componente` | Top-K grouping with `OTHER` | Rolling system counts | Same as above |
| `taller_planta_norm` | `$.taller_planta` | Uppercase, trim, double-space cleanup | Event-level carry | Workshop context may influence recurring issues |
| `taller_planta_grouped` | `$.taller_planta` | Top-K grouping with `OTHER` | Rolling workshop counts and recurrence | Workshop recurrence may capture localized operational patterns |
| `pauta_ejecutada_norm` | `$.pauta_ejecutada` | Uppercase, trim, accent removal | Event-level carry | Stable planned/executed program text |
| `pauta_ejecutada_grouped` | `$.pauta_ejecutada` | Top-K grouping with `OTHER` | Event-level carry | Lower-cardinality program context |
| `pauta_proyectada_norm` | `$.pauta_proyectada` | Uppercase, trim, accent removal | Stored in base | Stable planned maintenance program |
| `pauta_proyectada_grouped` | `$.pauta_proyectada` | Top-K grouping with `OTHER` | Stored in base | Lower-cardinality program context |
| `pauta_modelo_norm` | `$.pauta_ejecutada` or `$.pauta_proyectada` | Regex/token extraction (`FOTON`, `O500`, `B8RLE`, `ZK...`, `20000KM`) | Event carry and grouped context | Vehicle/program family can affect failure recurrence |
| `pauta_modelo_grouped` | Same as above | Top-K grouping with `OTHER` | Event-level carry | Makes model family usable for aggregation |
| `pauta_programa_norm` | `$.pauta_ejecutada` or `$.pauta_proyectada` | Regex extraction (`SMx`, `SARTx`, `20000KM`, `S`) | Event-level carry | Maintenance stage may influence near-term corrective probability |
| `pauta_programa_grouped` | Same as above | Top-K grouping with `OTHER` | Event-level carry | Lower-cardinality program context |
| `user_name_norm` | `$.user_name` | Uppercase, trim, accent removal | Event carry | Preserves operator/technician metadata already present in raw JSON |
| `unidad_negocio_norm` | `$.unidad_negocio` | Uppercase, trim | Event carry and rolling counts | Business unit may proxy route or usage profile |
| `keyword_*` | `$.repuestos[].repuesto_descripcion` | Binary keyword detection over cleaned text | Rolling keyword counts and recency | Part text reveals technical subsystem hints absent from raw categories |
| `tiene_repuestos` | `$.repuestos[]` | Binary from count > 0 | Event max and rolling recency | Presence of parts may indicate non-trivial repairs |
| `tiene_uuid_gestion` | `$.uuid_gestion[]` | Binary from count > 0 | Event max | Presence of management ids may indicate coordinated handling |
| `tiene_insumos` | `$.insumos[]` | Binary from count > 0 | Event max | Future-proofing for later raw data versions |

## New event-level columns

These are created in `merge_additional_event_fields()` after the original `create_eventos_dataframe()` output, so existing event logic remains unchanged.

- `filas_correctivo_evento`
- `uuid_gestion_count_evento`
- `uuid_gestion_unique_count_evento`
- `insumos_count_evento`
- `insumos_unique_count_evento`
- `repuestos_count_evento`
- `repuestos_cantidad_total_evento`
- `repuestos_codigos_unicos_evento`
- `repuestos_descripciones_unicas_evento`
- `duracion_ot_horas_prom_evento`
- `duracion_ot_horas_max_evento`
- `horas_desde_creacion_hasta_apertura_prom_evento`
- `km_desviacion_relativa_prom_evento`
- `repuestos_descripcion_texto_evento_clean`
- `repuestos_codigo_texto_evento`
- normalized event context fields such as `causa_origen_grouped`, `taller_planta_grouped`, `pauta_modelo_grouped`
- keyword/event flags such as `keyword_motor`, `keyword_freno`, `keyword_bateria`

## New predictive feature families

These are appended after the original feature creation. Existing features are not changed.

| Feature or pattern | Input columns | Transformation | Predictive hypothesis |
|---|---|---|---|
| `dias_desde_ultima_misma_causa` | `causa_origen_grouped`, `fecha_evento` | Group-wise lag in days | Repeated recent causes may recur quickly |
| `racha_misma_causa` | `causa_origen_grouped` | Consecutive same-cause streak length | Persistent unresolved causes can increase short-horizon risk |
| `num_causas_distintas_ult_30d` | `causa_origen_grouped`, `fecha_evento` | Rolling 30-day unique count | High recent cause diversity may indicate unstable buses |
| `count_causa_<categoria>_ult_3d/5d/7d` | `causa_origen_grouped`, `fecha_evento` | Rolling cause counts by bus | Cause-specific recurrence in short windows is directly predictive |
| `count_sistema_<categoria>_ult_7d/30d` | `sistema_componente_grouped`, `fecha_evento` | Rolling category counts | System-level recurrence can identify problematic subsystems when available |
| `count_taller_<categoria>_ult_7d/30d` | `taller_planta_grouped`, `fecha_evento` | Rolling category counts | Workshop recurrence may correlate with fleet clusters or repeated work |
| `count_unidad_<categoria>_ult_7d/30d` | `unidad_negocio_norm`, `fecha_evento` | Rolling category counts | Business-unit concentration may capture route/use intensity |
| `dias_desde_ultimo_mismo_taller` | `taller_planta_grouped`, `fecha_evento` | Group-wise lag in days | Repeated returns to the same plant may indicate unresolved issues |
| `racha_mismo_taller` | `taller_planta_grouped` | Consecutive same-workshop streak | Same-workshop loops can flag chronic repair cycles |
| `repuestos_count_ult_7d/30d` | `repuestos_count_evento`, `fecha_evento` | Rolling sums | Parts-heavy recent history may precede new failures |
| `repuestos_cantidad_total_ult_7d/30d` | `repuestos_cantidad_total_evento`, `fecha_evento` | Rolling sums | Larger recent material usage may proxy severity |
| `uuid_gestion_count_ult_7d/30d` | `uuid_gestion_count_evento`, `fecha_evento` | Rolling sums | Management-heavy history may proxy operational complexity |
| `filas_correctivo_ult_7d/30d` | `filas_correctivo_evento`, `fecha_evento` | Rolling sums | Multi-row same-day corrective events may indicate complex technical incidents |
| `dias_desde_ultimo_evento_con_repuestos` | `tiene_repuestos_evento`, `fecha_evento` | Recency to prior parts-consuming event | Recent parts consumption can precede new corrective events |
| `racha_eventos_con_repuestos` | `tiene_repuestos_evento` | Consecutive streak length | Repeated parts-heavy events may indicate unresolved degradation |
| `duracion_ot_horas_prom_evento_ult_7d/30d` | `duracion_ot_horas_prom_evento`, `fecha_evento` | Rolling sums | Longer recent repair effort may signal unstable assets |
| `num_keywords_tecnicos_ult_7d/30d` | `num_keywords_tecnicos_evento`, `fecha_evento` | Rolling sums | More subsystem hints in recent parts text can reflect broader deterioration |
| `count_keyword_<keyword>_ult_7d/30d` | `keyword_*`, `fecha_evento` | Rolling sums | Keyword-specific recurrence, e.g. brakes or batteries, can be highly actionable |
| `dias_desde_ultimo_keyword_motor/freno/bateria` | `keyword_*`, `fecha_evento` | Recency to prior keyword match | Recent subsystem-specific work can predict near-term repeat correctives |
| `correctivo_prox_10d` | `fecha_evento` | Future target window | Extends evaluation to medium-short horizon |
| `correctivo_prox_14d` | `fecha_evento` | Future target window | Adds two-week operational horizon |
| `correctivo_prox_30d` | `fecha_evento` | Future target window | Adds monthly maintenance planning horizon |

## Safety and quality checks added to the workflow

- `summarize_data_quality()` for cleaned base and event datasets
- `summarize_feature_quality()` for the feature matrix
- duplicate checks on `firebase_id` and `placa_patente + fecha_evento`
- missing timestamp counts on derived datetime columns
- category explosion monitoring through grouped/normalized category cardinality
- non-negative interval validation on `dias_desde_correctivo_anterior`
- no future leakage introduced in new rolling features; all new windows use current and past events only
