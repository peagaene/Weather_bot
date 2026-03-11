# Pre-Live Checklist

Documento auxiliar. O plano principal agora está em [MASTER_ROADMAP.md](C:/Bot_poly/Weather/MASTER_ROADMAP.md).

## Bloqueadores para live

- `python -m pytest -q` precisa passar no runtime operacional.
- `python run_smoke_tests.py` precisa passar sem falhas.
- `run_auto_trade.py --iterations 1` em dry-run precisa fechar sem exception.
- `run_weather_models.py --execute-top 0` precisa completar sem travar em providers ou CLOB.
- `run_latency_probe.py` não pode falhar em `account_snapshot`, `public_positions` ou `public_activity`.
- wallet/CLOB precisam mostrar saldo e allowance acima de `PAPERBOT_MIN_STAKE_USD`.
- replay gate precisa estar aprovado, a menos que o run continue explicitamente em modo de validação.

## Pode continuar só observando

- não há oportunidades liberadas, mas o ciclo completa sem erro.
- existem bloqueios de policy ou sizing, mas não de infraestrutura.
- dashboard carrega, reconcile roda e snapshots continuam sendo gravados.
- falhas isoladas de provider usam fallback e não derrubam o ciclo.

## Pode mandar ordem

- todos os bloqueadores acima estão verdes.
- pelo menos um scan dry-run recente gerou oportunidade `policy_allowed=True` e `plan_valid=True`.
- `signal_tier` e `confidence_tier` do candidato fazem sentido econômico na revisão manual.
- não há `provider_failures` core persistentes no mesmo ciclo.
- `price_source` vem de book executável, não de fallback degradado.
- stake, `order_min_size`, `tick_size` e limites diários estão coerentes com o bankroll.

## Nice to have antes de abrir live

- soak test de algumas horas em dry-run sem reinício manual.
- revisar visual final do dashboard em desktop e mobile.
- registrar thresholds atuais de policy e cobertura para facilitar rollback.
