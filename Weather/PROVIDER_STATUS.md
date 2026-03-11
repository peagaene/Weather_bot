# Provider Status

Documento auxiliar. O plano principal agora está em [MASTER_ROADMAP.md](C:/Bot_poly/Weather/MASTER_ROADMAP.md).

## Como ler
- `Funcionando`: já existe no código e participa do pipeline atual
- `Implementado, validar`: já existe no código, mas depende de chave/configuração/runtime
- `Implementado, observação`: já existe no código, mas hoje está sendo usado só para observação/truth
- `Ainda não implementado`: ainda não entrou no código

## O que aparece na imagem

### APIs / fontes que aparecem na imagem
| Fonte | Nome na imagem | Status no bot | Observação |
|---|---|---|---|
| Tomorrow.io | `Tomorrow.io` | Implementado, validar | Depende de `TOMORROW_API_KEY` |
| NOAA / NWS API | `NWS` | Funcionando | EUA |
| Open-Meteo | `Open-Meteo` | Funcionando | Base global principal |
| OpenWeather | `OpenWeather` | Implementado, validar | Depende de `OPENWEATHER_API_KEY` |
| WeatherAPI | `WeatherAPI` | Implementado, validar | Depende de `WEATHERAPI_KEY` |
| Visual Crossing | `VisCross` | Implementado, validar | Depende de `VISUAL_CROSSING_API_KEY` |
| Pirate Weather | `PirateWx` | Ainda não implementado | Equivale a `Pirate Weather` da sua lista |

### Modelos que aparecem na imagem
Esses aparecem na imagem, mas não são APIs separadas no nosso desenho atual.

| Modelo | Status no bot | Observação |
|---|---|---|
| GFS | Funcionando | Via Open-Meteo |
| ECMWF | Funcionando | Via Open-Meteo |
| ICON | Funcionando | Via Open-Meteo |
| JMA | Funcionando | Via Open-Meteo |
| GEM | Funcionando | Via Open-Meteo |
| HRRR | Funcionando | EUA, curto prazo |

## Lista completa baseada na sua lista

### APIs meteorológicas globais / comerciais
| Fonte | Status no bot | Observação |
|---|---|---|
| Open-Meteo | Funcionando | Forecast global |
| OpenWeather | Implementado, validar | Forecast/current com key |
| WeatherAPI | Implementado, validar | Forecast/current com key |
| Weatherbit | Ainda não implementado | Planejado |
| Meteosource | Ainda não implementado | Planejado |
| Visual Crossing | Implementado, validar | Forecast/history com key |
| Weatherstack | Ainda não implementado | Não priorizado ainda |
| Tomorrow.io | Implementado, validar | Forecast/current com key |
| meteoblue | Ainda não implementado | Planejado |
| AccuWeather | Ainda não implementado | Não priorizado ainda |
| The Weather Company | Ainda não implementado | Não priorizado ainda |
| Meteomatics | Ainda não implementado | Não priorizado ainda |
| Xweather | Ainda não implementado | Não priorizado ainda |

### APIs focadas em observações / estações
| Fonte | Status no bot | Observação |
|---|---|---|
| Xweather Observations | Ainda não implementado | Não priorizado ainda |
| The Weather Company Observations | Ainda não implementado | Não priorizado ainda |
| Weatherbit Current Weather | Ainda não implementado | Entraria junto com Weatherbit |
| Meteostat | Implementado, observação | Truth internacional, recém integrado |
| OpenWeather Current / One Call | Implementado, validar | Já entra pelo provider OpenWeather |

### Redes de estações pessoais / IoT
| Fonte | Status no bot | Observação |
|---|---|---|
| Netatmo Weather API | Ainda não implementado | Não priorizado |
| Tempest API | Ainda não implementado | Não priorizado |
| Ambient Weather API | Ainda não implementado | Não priorizado |

### APIs oficiais / serviços meteorológicos nacionais
| Fonte | Status no bot | Observação |
|---|---|---|
| NOAA / NWS API | Funcionando | Forecast + observação EUA |
| NOAA Climate Data Online (CDO) | Ainda não implementado | Bom para histórico/backfill |
| MET Norway API | Ainda não implementado | Planejado |
| FMI Open Data | Ainda não implementado | Não priorizado ainda |
| DWD Open Data | Ainda não implementado | Pode ser coberto por Bright Sky depois |
| Met Office DataHub | Ainda não implementado | Não priorizado ainda |
| Bureau of Meteorology (BOM) | Ainda não implementado | Não priorizado ainda |
| Japan Weather Association Weather API | Ainda não implementado | Não priorizado ainda |

### APIs e serviços de dados climáticos / reanálise / ciência
| Fonte | Status no bot | Observação |
|---|---|---|
| NASA POWER | Ainda não implementado | Baixa prioridade para intraday |
| ECMWF Web API / Open Data | Ainda não implementado | Hoje usamos ECMWF via Open-Meteo |

### Camadas simplificadas / wrappers sobre dados oficiais
| Fonte | Status no bot | Observação |
|---|---|---|
| Bright Sky | Ainda não implementado | Planejado, útil para Munique/Alemanha |
| Pirate Weather | Ainda não implementado | Equivale ao `PirateWx` da imagem |

## Resposta objetiva à sua pergunta

### O que aparece na imagem e não estava listado como nome exato de API
- `GFS`
- `ECMWF`
- `ICON`
- `JMA`
- `GEM`
- `HRRR`

Esses são modelos, não APIs comerciais separadas na forma como você listou.

### O que estava na sua lista e aparece na imagem com nome diferente
- `Visual Crossing` -> `VisCross`
- `Pirate Weather` -> `PirateWx`
- `NOAA / NWS API` -> `NWS`

## Próximos a adicionar
1. Meteostat
   - já integrado
   - falta validação em ambiente com dependência instalada
2. NOAA ISD
3. Weatherbit
4. Meteosource
5. MET Norway
6. Bright Sky
