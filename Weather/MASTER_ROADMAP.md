# Master Roadmap

## Objetivo

Levar o bot para um estado em que:
- o que ja funciona continue funcionando
- o live possa ficar online continuamente para coleta e validacao
- todas as cidades com mercado estejam em observacao
- todos os providers relevantes sejam implementados e validados gradualmente
- cidades novas so sejam liberadas para live com historico suficiente
- qualquer integracao nova tenha trilha clara de validacao, revisao e rollback

Documentos de apoio:
- [PRELIVE_CHECKLIST.md](C:/Bot_poly/Weather/PRELIVE_CHECKLIST.md)
- [DEGEN_FEATURE_PARITY.md](C:/Bot_poly/Weather/DEGEN_FEATURE_PARITY.md)
- [DATA_ENRICHMENT_PLAN.md](C:/Bot_poly/Weather/DATA_ENRICHMENT_PLAN.md)
- [CITY_EXPANSION_ROADMAP.md](C:/Bot_poly/Weather/CITY_EXPANSION_ROADMAP.md)
- [PROVIDER_EXPANSION_PLAN.md](C:/Bot_poly/Weather/PROVIDER_EXPANSION_PLAN.md)
- [PROVIDER_STATUS.md](C:/Bot_poly/Weather/PROVIDER_STATUS.md)
- [PROVIDER_ROLLOUT_POLICY.md](C:/Bot_poly/Weather/PROVIDER_ROLLOUT_POLICY.md)

## Etapa atual

Estamos na etapa de:
- estabilizacao do live atual
- expansao de providers em `observation_only`
- enriquecimento do banco para recalibracao automatica
- rollout automatico por provider com benchmark

Ainda nao estamos na etapa de:
- liberar cidades internacionais para live
- promover providers novos para `eligible_for_live_influence`

## Estado atual

### Nucleo operacional
- [x] scanner de weather
- [x] selecao e policy
- [x] execucao live
- [x] reconcile
- [x] dashboard
- [x] persistencia em banco
- [x] replay gate
- [x] coleta de oportunidades bloqueadas

### Dados e calibracao
- [x] `scan_predictions`
- [x] `forecast_source_snapshots`
- [x] `station_observation_daily_highs`
- [x] `market_history_snapshots`
- [x] `policy_profile.json`
- [x] `source_weight_profile.json`
- [x] `truth_weight_profile.json`
- [x] `provider_benchmark.json`
- [x] `provider_rollout_profile.json`

### Providers
- [x] Open-Meteo + GFS/ECMWF/ICON/GEM/JMA
- [x] NWS
- [x] MOS
- [x] HRRR
- [x] Tomorrow.io
- [x] WeatherAPI
- [x] Visual Crossing
- [x] OpenWeather
- [x] Meteostat
- [x] NOAA ISD
- [x] Weatherbit
- [x] Meteosource
- [x] MET Norway
- [x] Bright Sky
- [x] Pirate Weather
- [x] meteoblue
- [x] AccuWeather
- [x] Weatherstack
- [ ] Meteomatics
- [ ] Xweather
- [ ] The Weather Company
- [ ] FMI
- [ ] DWD
- [ ] Met Office
- [ ] BOM
- [ ] JWA
- [ ] NASA POWER
- [ ] ECMWF Open Data

### Cidades
- [x] cidades dos EUA em live/observacao
- [x] cidades internacionais em observacao
- [ ] primeira cidade internacional liberada para live

## Regras de operacao

- nao quebrar o que ja esta funcionando
- execucao e guiada por setup, nao por whitelist de cidade
- cidade sozinha nao libera ordem
- cidade sozinha nao bloqueia ordem se o setup for forte e os filtros estruturais passarem
- mudancas novas entram no live apenas no proximo restart

## Criterio de nao regressao

Uma entrega so avanca se:
- [ ] testes focados passam
- [ ] `py_compile` passa
- [ ] scan manual completa
- [ ] persistencia nova no banco grava corretamente
- [ ] nenhum fluxo live atual regride

## Trilhas abertas

### 1. Estabilidade do live
- [ ] monitorar fills e rejeicoes
- [ ] monitorar erros de provider e Gamma
- [ ] acompanhar `weather_model_latest.json`
- [ ] revisar bloqueios dominantes por janela

### 2. Cobertura e liberacao por cidade
- [x] adicionar todas as cidades conhecidas
- [x] suportar mercados em `F` e `C`
- [x] manter cidades novas em `observation_only`
- [ ] validar slugs e unidades com historico real
- [ ] acumular observacoes minimas por cidade
- [ ] gerar recomendacao automatica por cidade
- [ ] liberar primeira cidade internacional

### 3. Expansao de providers
- [x] benchmark por provider
- [x] rollout profile automatico
- [x] runtime respeita `provider_rollout_profile.json`
- [ ] acumular historico dos providers novos
- [ ] promover providers novos para `eligible_for_weighting`
- [ ] implementar proximo batch do roadmap

### 4. Policy e calibracao
- [x] weights por source/cidade/regime
- [x] weights por truth observado
- [x] rollout automatico para providers opcionais
- [ ] recalibracao com dados internacionais
- [ ] policy city-specific para cidades novas

### 5. Dashboard e observabilidade
- [x] win rate do bot
- [x] metricas de forecast por source
- [x] recomendacoes automaticas por cidade/day_label
- [ ] painel de rollout por provider
- [ ] painel de rollout por cidade

## Proximo passo recomendado

1. Reiniciar o weather bot para usar o `provider_rollout_profile.json`
2. Deixar coletar mais dados com os providers novos
3. Rerodar `run_provider_benchmark.py`
4. Promover providers novos que aparecerem com amostra suficiente
5. So depois seguir para o proximo batch de implementacao
