## Data Enrichment Plan

Documento auxiliar. O plano principal agora está em [MASTER_ROADMAP.md](C:/Bot_poly/Weather/MASTER_ROADMAP.md).

Objetivo: enriquecer o banco local com histórico reutilizável para calibrar o bot sem alterar a decisão operacional do scan atual.

### Prioridade

1. Persistir previsões por source/modelo em granularidade de run.
2. Persistir observações oficiais por estação para truth operacional.
3. Backfill opcional para preencher janelas históricas.
4. Usar essas tabelas para calibrar pesos, thresholds e replay.

### Escopo da fase 1

Implementado nesta fase:

- tabela `forecast_source_snapshots`
- tabela `station_observation_daily_highs`
- captura automática no `run_weather_models.py`
- persistência fail-open: falha de enriquecimento não derruba o scan

### Tabelas

#### `forecast_source_snapshots`

Uma linha por source/modelo e por oportunidade analisada no run.

Campos principais:

- `run_id`
- `captured_at`
- `city_key`
- `day_label`
- `date_str`
- `event_slug`
- `market_slug`
- `bucket`
- `side`
- `source_name`
- `forecast_temp_f`
- `effective_weight`
- `aligns_with_trade_side`
- `source_in_bucket`
- `source_delta_f`

Uso:

- ranking histórico por source/cidade/horizonte
- análise de concordância por source
- calibração futura de pesos e policy

#### `station_observation_daily_highs`

Uma linha por cidade e data local observada, derivada da estação oficial do mercado.

Campos principais:

- `captured_at`
- `city_key`
- `station_id`
- `local_date`
- `observed_high_f`

Uso:

- truth operacional por cidade/data
- comparação com sources/modelos
- backfill de métricas por regime e horizonte

### Regras operacionais

- o scan continua armazenando tudo em `scan_predictions`
- o enriquecimento é auxiliar e nunca bloqueia o scan
- as novas tabelas servem para análise e calibração posterior

### Próximas fases

1. script de backfill para observações oficiais
2. script de análise cruzando `forecast_source_snapshots` com truth
3. geração automática de pesos por source/cidade/horizonte usando essas tabelas
