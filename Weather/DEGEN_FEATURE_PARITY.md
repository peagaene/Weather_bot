# DegenDoppler Feature Parity

## Objetivo

Levar o bot principal a, no minimo, o mesmo conjunto funcional observado no DegenDoppler para weather markets, sem degradar o operacional live.

## Matriz

| Feature | Degen | Nosso bot | Status | Acao |
|---|---|---|---|---|
| Scanner de mercados weather | Sim | Sim | OK | manter |
| Direcao `YES/NO` por mercado | Sim | Sim | OK | manter |
| Edge por preco atual | Sim | Sim | OK | manter |
| Worst-case edge | Sim | Sim | OK | dar mais destaque |
| Consenso `x/y` | Sim | Parcial | Em andamento | expor `agreement_summary` no payload/dashboard |
| Modelos concordantes visiveis | Sim | Parcial | Em andamento | expor `agreeing_model_names` |
| Tiers tipo `SAFE/NEAR/STRONG` | Sim | Parcial | Parcial | mapear melhor nossos tiers e comparar regras |
| NWS | Sim | Sim | OK | manter |
| OpenWeather | Sim | Sim | OK | validar com key ativa |
| WeatherAPI | Sim | Sim | OK | validar com key ativa |
| Tomorrow.io | Sim | Sim | OK | validar com key ativa e economizar limite |
| Visual Crossing | Nao evidente no print, mas util | Sim | OK | validar com key ativa |
| MOS | Sim | Nao | Faltando | implementar fonte estavel e parser |
| HRRR | Sim | Nao | Faltando | implementar pipeline de curto prazo |
| Tratamento especial para `today` | Sim | Parcial | Parcial | reforcar ao entrar HRRR/MOS |
| Dashboard com scanner comparavel | Sim | Parcial | Em andamento | reforcar consenso, modelos e edge |
| Execucao live | Nao evidente | Sim | Melhor que o Degen | manter |

## Prioridade real

1. Consolidar providers ja suportados com as novas keys.
2. Subir o consenso real de `1/5` para algo mais robusto.
3. Expor consenso e modelos no dashboard.
4. Implementar `MOS`.
5. Implementar `HRRR`.

## Regra

Enquanto o bot principal estiver rodando live:
- preferir mexer primeiro em consenso, coleta e visualizacao
- nao reabrir a camada de execucao sem necessidade clara
