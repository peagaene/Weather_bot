# Weather Bot Feature Roadmap

## Objetivo

Aproximar o bot do perfil de consenso observado no DegenDoppler sem degradar o operacional live.

## Fase 1: Providers ja suportados

Status: em andamento

- Ativar e validar:
  - `WEATHERAPI_KEY`
  - `OPENWEATHER_API_KEY`
  - `TOMORROW_API_KEY`
  - `VISUAL_CROSSING_API_KEY`
- Manter `WEATHER_AUTO_TRADE_INTERVAL_SECONDS=300`
- Evitar scan manual no dashboard
- Observar:
  - `valid_model_count`
  - `coverage_issue_type`
  - `provider_failures`

Meta:
- Sair do padrao `1/5` em janelas normais
- Reduzir `mixed_rate_limited`

## Fase 2: Consensus comparable ao Degen

Status: iniciado

- Expor melhor no dashboard:
  - `agreement_models/total_models`
  - `worst-case edge`
  - `signal_tier`
- Comparar:
  - mesmo mercado
  - mesmo lado
  - mesma forca de consenso

Meta:
- Saber se estamos "parelhos" na descoberta antes de ajustar tiers

Regra atual de fallback:
- se apenas `4` modelos confiaveis estiverem disponiveis por falha externa de provider, operamos em cima de `4/4`
- se `5` estiverem disponiveis, a referencia volta a ser `5/5`
- o fallback nao aceita consenso parcial (`3/4`, `4/5`)

## Fase 3: Nova fonte oficial adicional

Status: planejado

Prioridade:
1. fonte oficial de guidance com maxima diaria viavel
2. so depois `MOS`

Observacao:
- `MOS` nao deve ser adicionado sem definir fonte oficial estavel e parsing confiavel
- `HRRR` tambem exige integracao propria e testes de horizonte curto

## Fase 4: MOS

Status: planejado

Necessario:
- escolher fonte oficial/formatavel
- extrair maxima diaria por cidade/data
- integrar ao `ModelForecast`
- calibrar peso proprio

## Fase 5: HRRR / curto prazo

Status: planejado

Necessario:
- fonte oficial ou pipeline robusto
- granularidade horaria confiavel
- regra de peso especial para `today`

## Regra de seguranca

Enquanto o bot principal estiver operando live:
- nao alterar fluxo de ordem/execution sem necessidade
- preferir mexer em:
  - coleta de dados
  - consenso
  - dashboard
  - observabilidade
