# Provider Rollout Policy

Documento auxiliar. O plano principal continua em [MASTER_ROADMAP.md](C:/Bot_poly/Weather/MASTER_ROADMAP.md).

## Objetivo

Definir como um provider novo sai de implementado para influenciar pesos, policy e live sem degradar o que ja funciona.

## Estagios

### `observation_only`
- coleta ligada
- entra no banco
- nao influencia live por decisao de rollout

### `eligible_for_weighting`
- ja tem amostra minima
- erro e vies por cidade/horizonte estao medidos
- pode influenciar `source_weight_profile` e `truth_weight_profile`

### `eligible_for_live_influence`
- ja mostrou valor no ensemble
- melhora ou mantem `coverage_score`, `consensus_score` e `win rate`
- pode participar do setup que vai para live

### `rejected_or_low_value`
- adiciona ruido
- piora consenso ou qualidade
- fica desligado ou so como observacao historica

## Criterios minimos por stage

### Para sair de `observation_only`
- 25+ snapshots uteis em `forecast_source_snapshots`
- 10+ dias com truth observada quando for provider de observation/truth
- sem falha estrutural recorrente de autenticacao / SSL / schema

### Para sair de `eligible_for_weighting`
- amostra suficiente por pelo menos uma combinacao `city/day_label`
- MAE/RMSE/Bias nao pior que os providers ja estaveis do mesmo grupo
- nao aumentar materialmente `confidence_risky`

### Para sair de `eligible_for_live_influence`
- nao piorar o conjunto de sinais aprovados
- manter ou melhorar o win rate do segmento alvo
- nao introduzir degradacao operacional no live

## Status atual

| Provider | Stage | Observacao |
|---|---|---|
| Open-Meteo | eligible_for_live_influence | base global principal |
| GFS / ECMWF / ICON / GEM / JMA via Open-Meteo | eligible_for_live_influence | base do ensemble |
| NWS | eligible_for_live_influence | core EUA |
| MOS | eligible_for_live_influence | core EUA |
| HRRR | eligible_for_live_influence | core EUA curto prazo |
| Tomorrow.io | observation_only | key ok; unidade corrigida; precisa re-baseline |
| WeatherAPI | observation_only | key ok; request real validado |
| Visual Crossing | observation_only | key ok; request real validado |
| OpenWeather | observation_only | key ok; request real validado |
| Meteostat | observation_only | truth internacional |
| NOAA ISD | observation_only | truth internacional |
| Weatherbit | observation_only | implementado; key em provisioning |
| Meteosource | observation_only | key ok; request real validado |
| MET Norway | observation_only | implementado; ainda sensivel a SSL/runtime |
| Bright Sky | observation_only | escopo regional, Munique |
| Pirate Weather | observation_only | key ok; request real validado |
| meteoblue | observation_only | key ok; request real validado |
| AccuWeather | observation_only | key ok; request real validado |
| Weatherstack | rejected_or_low_value | plano atual nao suporta forecast |
| Meteomatics | not_implemented | pendente |
| Xweather | not_implemented | pendente |
| The Weather Company | not_implemented | pendente |

## Profile automatico

- O benchmark gera [provider_rollout_profile.json](C:/Bot_poly/Weather/export/analysis/provider_rollout_profile.json)
- O runtime aplica esse profile apenas aos providers opcionais e novos
- Providers core atuais continuam preservados contra degradacao automatica por benchmark parcial

## Regra operacional

- provider novo nao justifica afrouxar policy por si so
- provider novo so entra no live quando melhorar setup, nao por "estar disponivel"
- rollout sempre por evidencia de banco, nao por sensacao de cobertura
- provider com problema estrutural de plano ou produto inadequado pode ir direto para `rejected_or_low_value`
