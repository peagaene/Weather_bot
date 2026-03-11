# Provider Status

Documento auxiliar. O plano principal continua em [MASTER_ROADMAP.md](C:/Bot_poly/Weather/MASTER_ROADMAP.md).

## Como ler
- `Funcionando`: existe no codigo e participa do pipeline atual
- `Implementado, validar`: existe no codigo, mas ainda precisa de validacao operacional completa
- `Implementado, observacao`: existe no codigo, mas hoje fica em observacao/truth
- `Ainda nao implementado`: ainda nao entrou no codigo

## O que aparece na imagem

### APIs / fontes que aparecem na imagem
| Fonte | Nome na imagem | Status no bot | Observacao |
|---|---|---|---|
| Tomorrow.io | `Tomorrow.io` | Implementado, validar | Key ok; unidade corrigida; precisa re-baseline no banco |
| NOAA / NWS API | `NWS` | Funcionando | EUA |
| Open-Meteo | `Open-Meteo` | Funcionando | Base global principal |
| OpenWeather | `OpenWeather` | Implementado, validar | Key ok; request real validado |
| WeatherAPI | `WeatherAPI` | Implementado, validar | Key ok; request real validado |
| Visual Crossing | `VisCross` | Implementado, validar | Key ok; request real validado |
| Pirate Weather | `PirateWx` | Implementado, validar | Key ok; request real validado |

### Modelos que aparecem na imagem
Esses aparecem na imagem, mas nao sao APIs separadas no desenho atual.

| Modelo | Status no bot | Observacao |
|---|---|---|
| GFS | Funcionando | Via Open-Meteo |
| ECMWF | Funcionando | Via Open-Meteo |
| ICON | Funcionando | Via Open-Meteo |
| JMA | Funcionando | Via Open-Meteo |
| GEM | Funcionando | Via Open-Meteo |
| HRRR | Funcionando | EUA, curto prazo |

## Lista completa baseada na sua lista

### APIs meteorologicas globais / comerciais
| Fonte | Status no bot | Observacao |
|---|---|---|
| Open-Meteo | Funcionando | Forecast global |
| OpenWeather | Implementado, validar | Key ok; request real validado |
| WeatherAPI | Implementado, validar | Key ok; request real validado |
| Weatherbit | Implementado, validar | Key em provisioning; `403` temporario |
| Meteosource | Implementado, validar | Key ok; request real validado |
| Visual Crossing | Implementado, validar | Key ok; request real validado |
| Weatherstack | Implementado, validar | Key ok; plano atual nao suporta forecast |
| Tomorrow.io | Implementado, validar | Key ok; unidade corrigida; precisa re-baseline |
| meteoblue | Implementado, validar | Key ok; request real validado |
| AccuWeather | Implementado, validar | Key ok; request real validado |
| The Weather Company | Ainda nao implementado | Nao priorizado ainda |
| Meteomatics | Ainda nao implementado | Nao priorizado ainda |
| Xweather | Ainda nao implementado | Nao priorizado ainda |

### APIs focadas em observacoes / estacoes
| Fonte | Status no bot | Observacao |
|---|---|---|
| Xweather Observations | Ainda nao implementado | Nao priorizado ainda |
| The Weather Company Observations | Ainda nao implementado | Nao priorizado ainda |
| Weatherbit Current Weather | Implementado, validar | Entrou junto com Weatherbit |
| Meteostat | Implementado, observacao | Truth internacional |
| OpenWeather Current / One Call | Implementado, validar | Ja entra pelo provider OpenWeather |

### Redes de estacoes pessoais / IoT
| Fonte | Status no bot | Observacao |
|---|---|---|
| Netatmo Weather API | Ainda nao implementado | Nao priorizado |
| Tempest API | Ainda nao implementado | Nao priorizado |
| Ambient Weather API | Ainda nao implementado | Nao priorizado |

### APIs oficiais / servicos meteorologicos nacionais
| Fonte | Status no bot | Observacao |
|---|---|---|
| NOAA / NWS API | Funcionando | Forecast + observacao EUA |
| NOAA ISD | Implementado, observacao | Truth global fail-open para cidades internacionais |
| NOAA Climate Data Online (CDO) | Ainda nao implementado | Bom para historico/backfill |
| MET Norway API | Implementado, validar | Forecast global sem key |
| FMI Open Data | Ainda nao implementado | Nao priorizado ainda |
| DWD Open Data | Ainda nao implementado | Pode ser coberto por Bright Sky depois |
| Met Office DataHub | Ainda nao implementado | Nao priorizado ainda |
| Bureau of Meteorology (BOM) | Ainda nao implementado | Nao priorizado ainda |
| Japan Weather Association Weather API | Ainda nao implementado | Nao priorizado ainda |

### APIs e servicos de dados climaticos / reanalise / ciencia
| Fonte | Status no bot | Observacao |
|---|---|---|
| NASA POWER | Ainda nao implementado | Baixa prioridade para intraday |
| ECMWF Web API / Open Data | Ainda nao implementado | Hoje usamos ECMWF via Open-Meteo |

### Camadas simplificadas / wrappers sobre dados oficiais
| Fonte | Status no bot | Observacao |
|---|---|---|
| Bright Sky | Implementado, validar | Restrito a Munique/Alemanha por enquanto |
| Pirate Weather | Implementado, validar | Equivale ao `PirateWx` da imagem |

## Resposta objetiva a pergunta da imagem

### O que aparece na imagem e nao estava listado como nome exato de API
- `GFS`
- `ECMWF`
- `ICON`
- `JMA`
- `GEM`
- `HRRR`

Esses sao modelos, nao APIs comerciais separadas.

### O que aparece na imagem com nome diferente
- `Visual Crossing` -> `VisCross`
- `Pirate Weather` -> `PirateWx`
- `NOAA / NWS API` -> `NWS`

## Proximos a adicionar
1. Meteomatics
2. Xweather
3. The Weather Company
4. FMI
5. DWD
6. Met Office
7. BOM
8. JWA
9. NASA POWER
10. ECMWF Open Data
