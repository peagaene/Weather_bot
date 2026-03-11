# Provider Expansion Plan

Documento auxiliar. O plano principal continua em [MASTER_ROADMAP.md](C:/Bot_poly/Weather/MASTER_ROADMAP.md).

## Estado atual

Estamos no meio da fase de expansao de providers.

Ja implementados:
- Meteostat
- NOAA ISD
- Weatherbit
- Meteosource
- MET Norway
- Bright Sky
- Pirate Weather
- meteoblue
- AccuWeather
- Weatherstack

Validados com request real:
- Tomorrow.io
- OpenWeather
- WeatherAPI
- Visual Crossing
- Pirate Weather
- meteoblue
- Meteosource
- AccuWeather

Pendencias conhecidas:
- Weatherbit: key ainda em provisioning
- Weatherstack: plano atual nao suporta forecast
- Tomorrow.io: precisa de re-baseline por historico antigo com unidade contaminada
- Providers novos ainda sem amostra suficiente no banco para promotion de weighting/live

## Ordem restante

### Fase 1: truth / observacao
1. `Meteostat`
   - Status: implementado
   - Falta: aumentar amostra internacional e validar comparacao por cidade

2. `NOAA ISD`
   - Status: implementado
   - Falta: consolidar cobertura real por cidade internacional

### Fase 2: forecast global
3. `Weatherbit`
   - Status: implementado
   - Falta: key ativa

4. `Meteosource`
   - Status: implementado e validado
   - Falta: ganhar amostra no banco

5. `MET Norway`
   - Status: implementado
   - Falta: validacao operacional de rede/SSL por cidade

6. `meteoblue`
   - Status: implementado e validado
   - Falta: ganhar amostra no banco

7. `AccuWeather`
   - Status: implementado e validado
   - Falta: ganhar amostra no banco

8. `Pirate Weather`
   - Status: implementado e validado
   - Falta: ganhar amostra no banco

### Fase 3: enterprise / opcional
9. `Meteomatics`
10. `Xweather`
11. `The Weather Company`

### Fase 4: nacionais / cientificos
12. `FMI`
13. `DWD`
14. `Met Office`
15. `BOM`
16. `JWA`
17. `NASA POWER`
18. `ECMWF Open Data`

## Regra de rollout

- Provider novo entra primeiro em `observation_only`
- Provider so sobe para `eligible_for_weighting` quando aparecer bem em:
  - [provider_benchmark.json](C:/Bot_poly/Weather/export/analysis/provider_benchmark.json)
  - [provider_rollout_profile.json](C:/Bot_poly/Weather/export/analysis/provider_rollout_profile.json)
- Provider so pode influenciar live quando nao piorar:
  - consenso
  - coverage
  - win rate
  - operacao do scanner/live

## Proximo passo recomendado

1. Deixar o bot coletar com os providers novos
2. Rerodar `run_provider_benchmark.py`
3. Promover os novos providers que aparecerem com amostra suficiente
4. So depois seguir para o proximo batch de implementacao
