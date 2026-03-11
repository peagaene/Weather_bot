# Master Roadmap

## Objetivo

Levar o bot para um estado em que:

- o que já funciona continue funcionando;
- o live possa ficar online de forma contínua para coletar dados e validar o operacional atual;
- todas as cidades com mercado estejam em observação;
- todos os providers e modelos relevantes sejam implementados e validados gradualmente;
- cidades novas só sejam liberadas para live quando houver histórico suficiente;
- qualquer nova implementação tenha trilha clara de validação, revisão e rollback.

Este arquivo é o plano principal. Os demais `.md` continuam existindo como documentos de apoio:

- [PRELIVE_CHECKLIST.md](C:/Bot_poly/Weather/PRELIVE_CHECKLIST.md)
- [DEGEN_FEATURE_PARITY.md](C:/Bot_poly/Weather/DEGEN_FEATURE_PARITY.md)
- [DATA_ENRICHMENT_PLAN.md](C:/Bot_poly/Weather/DATA_ENRICHMENT_PLAN.md)
- [CITY_EXPANSION_ROADMAP.md](C:/Bot_poly/Weather/CITY_EXPANSION_ROADMAP.md)
- [PROVIDER_EXPANSION_PLAN.md](C:/Bot_poly/Weather/PROVIDER_EXPANSION_PLAN.md)
- [PROVIDER_STATUS.md](C:/Bot_poly/Weather/PROVIDER_STATUS.md)

## Regras de operação

### Regra principal
- Não quebrar o que já está funcionando.

### Enquanto expandimos o bot
- O live pode continuar online.
- Não reiniciar o live no meio de uma integração.
- Toda mudança nova entra só no próximo restart.
- Toda integração nova precisa passar por:
  - teste focado
  - `py_compile`
  - scan manual
  - validação de persistência no banco

### O que não deve mudar sem necessidade clara
- execução live
- sizing live já validado
- recover/reconcile
- fluxo de persistência principal

## Estado atual

### Núcleo operacional
- [x] scanner de weather
- [x] seleção e policy
- [x] execução live
- [x] reconcile
- [x] dashboard
- [x] persistência em banco
- [x] replay gate
- [x] coleta de oportunidades bloqueadas

### Modelos e providers atuais
- [x] Open-Meteo
- [x] GFS via Open-Meteo
- [x] ECMWF via Open-Meteo
- [x] ICON via Open-Meteo
- [x] GEM via Open-Meteo
- [x] JMA via Open-Meteo
- [x] NWS
- [x] MOS
- [x] HRRR
- [x] Tomorrow.io
- [x] WeatherAPI
- [x] Visual Crossing
- [x] OpenWeather
- [x] Meteostat para observação internacional

### Cidades
- [x] cidades dos EUA em live/observação
- [x] cidades internacionais adicionadas em observação
- [ ] cidades internacionais liberadas para live

## Critério de não regressão

Uma entrega só avança se:

- [ ] `pytest` focado passar
- [ ] `py_compile` passar
- [ ] `run_weather_models.py --execute-top 0 --json` completar
- [ ] persistência nova no banco estiver gravando
- [ ] o motivo de bloqueio de cidades novas continuar sendo `city_observation_only` até liberação formal
- [ ] nenhum fluxo live atual regredir

## Trilhas de trabalho

### 1. Estabilidade do live atual
Objetivo: manter o bot online, coletando e operando o que já está validado.

Checklist:
- [ ] monitorar fills e rejeições
- [ ] monitorar erros de CLOB
- [ ] monitorar erros de provider
- [ ] acompanhar `weather_model_latest.json`
- [ ] acompanhar `weather_bot.db`
- [ ] revisar bloqueios dominantes por janela

Revisão pontual:
- Data:
- Achado:
- Ação:

### 2. Cobertura de todas as cidades
Objetivo: manter todos os mercados em observação e liberar live cidade por cidade.

Checklist:
- [x] adicionar todas as cidades conhecidas do Polymarket
- [x] suportar mercados em `°F` e `°C`
- [x] bloquear cidades novas em `observation only`
- [ ] validar slugs e unidades de todas as cidades com histórico real
- [ ] acumular histórico observado mínimo por cidade
- [ ] gerar recomendação de policy por cidade
- [ ] liberar primeira cidade internacional para live

Critério de liberação por cidade:
- [ ] 25+ observações válidas
- [ ] erro de forecast estável
- [ ] recomendação deixa de ser `block`
- [ ] revisão manual aprovada

Revisão pontual:
- Cidade:
- Status:
- Motivo do bloqueio:
- Próximo passo:

### 3. Expansão de providers
Objetivo: implementar um provider por vez, com validação e sem interromper o live atual.

#### Fase 1: truth / observação
- [x] Meteostat
- [ ] NOAA ISD

#### Fase 2: novos forecasts globais
- [ ] Weatherbit
- [ ] Meteosource
- [ ] MET Norway

#### Fase 3: reforço regional
- [ ] Bright Sky
- [ ] meteoblue

#### Fase 4: comerciais opcionais
- [ ] AccuWeather
- [ ] Weatherstack
- [ ] Meteomatics
- [ ] Xweather
- [ ] The Weather Company

Template por provider:
- Provider:
- Tipo: forecast / observation / history
- Cidades alvo:
- Chave necessária:
- Status: não iniciado / em implementação / validando / funcionando
- Banco afetado:
- Testes:
- Observações:

### 4. Enriquecimento de dados
Objetivo: continuar alimentando o banco para melhorar pesos, policy e cidade por cidade.

Checklist:
- [x] `scan_predictions`
- [x] `forecast_source_snapshots`
- [x] `station_observation_daily_highs`
- [x] `market_history_snapshots`
- [x] `policy_profile.json`
- [x] `source_weight_profile.json`
- [x] `truth_weight_profile.json`
- [ ] backfill internacional de observações
- [ ] análise consolidada por cidade/source/day_label
- [ ] recomendação automática de liberação por cidade

Revisão pontual:
- Tabela:
- Problema:
- Ação:

### 5. Policy e calibração
Objetivo: ampliar cobertura sem soltar entrada ruim.

Checklist:
- [x] bloqueio histórico de cidades ruins
- [x] overrides cirúrgicos para casos fortes
- [x] weights por source/cidade/regime
- [x] weights por truth observado
- [ ] recalibração com dados internacionais
- [ ] policy city-specific para cidades novas
- [ ] policy day-label específica por cidade nova

Regra:
- Não afrouxar policy global para liberar cidade nova.
- Liberar por segmentação e evidência.

### 6. Dashboard e observabilidade
Objetivo: acompanhar o que o bot está fazendo sem esconder risco.

Checklist:
- [x] win rate do bot
- [x] recent trades finalizados
- [x] paginação de open positions / recent trades
- [x] gráfico de P&L simplificado
- [x] painéis de MAE / RMSE / Bias
- [x] recomendações automáticas por cidade/day_label
- [ ] painel de rollout por cidade
- [ ] painel de rollout por provider
- [ ] painel de qualidade por cidade nova

### 7. Paridade e melhorias do Degen
Objetivo: igualar ou superar tudo que for útil no Degen sem piorar o live.

Checklist:
- [x] scanner
- [x] edge
- [x] worst-case edge
- [x] live execution
- [ ] consenso mais visível no dashboard
- [ ] agreeing models mais visível no dashboard
- [ ] tiers mais alinhados e explicáveis
- [ ] tratamento `today` ainda mais robusto com novos providers

## Plano de execução recomendado

### Passo 1
- manter live atual online
- não mexer em execução
- seguir alimentando banco

### Passo 2
- terminar validação do `Meteostat` em ambiente com dependência instalada
- confirmar gravação em cidades internacionais

### Passo 3
- integrar `NOAA ISD`
- comparar truth entre `Meteostat` e `ISD`

### Passo 4
- integrar `Weatherbit`
- validar como novo forecast global

### Passo 5
- integrar `Meteosource`
- comparar com `Weatherbit` e stack atual

### Passo 6
- integrar `MET Norway`
- usar como fallback gratuito

### Passo 7
- liberar primeira cidade internacional para live

## Checklist de cada entrega

- [ ] código implementado
- [ ] testes focados passando
- [ ] scan manual passando
- [ ] banco sendo alimentado
- [ ] motivo de bloqueio coerente
- [ ] revisão pontual documentada
- [ ] próximo passo definido

## Espaço para revisão contínua

### Revisão 1
- Data:
- Tema:
- O que mudou:
- O que quebrou:
- O que ficou pendente:

### Revisão 2
- Data:
- Tema:
- O que mudou:
- O que quebrou:
- O que ficou pendente:

### Revisão 3
- Data:
- Tema:
- O que mudou:
- O que quebrou:
- O que ficou pendente:

## Espaço para novas ideias

- [ ] adicionar nova cidade
- [ ] adicionar novo provider
- [ ] adicionar nova tabela
- [ ] revisar rollout de live
- [ ] revisar thresholds de policy
- [ ] revisar dashboard
- [ ] revisar replay/backtest
