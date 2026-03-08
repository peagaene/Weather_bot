# Paper Bot (Polymarket-style) - MVP de testes sem dinheiro real

Repositório focado em simulação (`paper`) com arquitetura de produção (feed, estratégia, risco, execução e painel simples).

## Componentes prontos

- `src/paperbot/main.py`: CLI de execução da simulação.
- `src/paperbot/feeds.py`: `PolymarketFeed` (preço real via REST Polymarket) e `SyntheticFeed` (fallback local).
- `src/paperbot/strategy.py`: estratégia de momentum.
- `src/paperbot/risk.py`: controle de risco (stop loss, limites, rejeições).
- `src/paperbot/execution.py`: execução em ledger interno (paper).
- `src/paperbot/engine.py`: orquestração tick -> sinal -> risco -> execução -> histórico.
- `src/paperbot/state.py`: estado da sessão e exposição.
- `src/paperbot/dashboard.py`: visualização (somente leitura dos exports).
- `.env.example`: parâmetros base e fonte de dados.

## Como rodar

```bash
python run_paper.py --ticks 200 --symbols BTCUSDC,ETHUSDC --polymarket-token-map BTCUSDC:0xABC...,ETHUSDC:0xDEF...
```

Para gerar sinal direcional em tempo real para BTC/ETH:

```bash
python run_signal.py --product-id BTC-USD --horizons 5,15
```

### Modo de feed

- Padrão atual: `polymarket`.
- Recomendado deixar `POLYMARKET_TOKEN_MAP` preenchido com os token ids.
- Para voltar ao gerador sintético local:

```bash
python run_paper.py --feed-mode synthetic --ticks 200 --symbols BTCUSDC,ETHUSDC
```

### Dashboard (somente leitura)

```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

A simulação continua sendo paper: não há envio de ordens reais.

## Scanner Degen Doppler -> Polymarket

O repositório também possui um scanner para mercados de clima do `degendoppler.com`.
Ele replica o cálculo rápido do site usando:

- `Open-Meteo` para forecast
- `/api/markets` do próprio Degen Doppler para os buckets do Polymarket
- sizing via `fractional Kelly`

Exemplo:

```bash
python run_degendoppler.py --min-edge 12 --top 5 --export-json export/degendoppler_scan.json
```

Saída:

- oportunidades ranqueadas
- `token_id` de `YES` ou `NO` para cada mercado
- plano de ordem sugerido com `stake_usd` e `limit_price_cents`

Observação: o script ainda trabalha em `dry-run` e não envia ordens reais. Isso evita acoplar o projeto a chaves privadas antes de validarmos o scanner.

## Scanner Direto de Modelos -> Polymarket

O modo recomendado agora é usar previsões diretas dos modelos, sem depender do Degen Doppler como fonte principal.

O comando novo:

```bash
python run_weather_models.py --top 5 --min-edge 10 --min-consensus 0.35
```

Esse fluxo usa:

- `Open-Meteo` com múltiplos modelos (`best_match`, `GFS`, `ECMWF`, `ICON`, `GEM`, `JMA`)
- `weather.gov` (`NWS`) como fonte oficial adicional
- `Gamma API` do Polymarket para descobrir os buckets e `token_id`

O ensemble calcula:

- temperatura prevista combinada
- spread entre modelos
- `sigma` adaptativo
- `consensus_score` para filtrar cenários com muita divergência

O scanner antigo `run_degendoppler.py` continua disponível como referência/benchmark.

### Filtros e risco

O comando novo agora aceita filtros práticos para operar de forma mais conservadora:

```bash
python run_weather_models.py --top 5 --min-edge 15 --min-consensus 0.50 --max-price-cents 60 --max-spread 3.0
```

Filtros úteis:

- `--min-price-cents`
- `--min-edge`
- `--min-model-prob`
- `--min-consensus`
- `--max-price-cents`
- `--max-spread`
- `--max-share-size`
- `--max-orders-per-event`

### Histórico persistente

Por padrão cada execução salva:

- CSV append-only: `export/history/weather_model_scan_log.csv`
- último snapshot JSON: `export/history/weather_model_latest.json`

Você pode desativar isso com:

```bash
python run_weather_models.py --no-history
```

### Dry-run e live

O fluxo padrão é `dry-run`: ele gera o plano e simula a execução sem enviar ordem.

Para executar só os melhores planos em modo real:

```bash
python run_weather_models.py --live --execute-top 1
```

Para o modo `live`, preencha no `.env`:

- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_API_KEY`
- `POLYMARKET_API_SECRET`
- `POLYMARKET_API_PASSPHRASE`

Se as credenciais de API não estiverem preenchidas, o cliente tenta derivá-las automaticamente a partir da chave privada.

### Proteções operacionais

O projeto agora também suporta:

- limite de ordens `live` por dia
- cooldown por cidade
- cooldown por evento
- cooldown por bucket/side
- estado persistente em `export/state/trading_state.json`

Exemplo:

```bash
python run_weather_models.py --live --execute-top 1 --daily-live-limit 3 --bucket-cooldown-minutes 360
```

### Cancel / Replace

Para cancelar ordens abertas parecidas e repostar no novo preço:

```bash
python run_weather_models.py --live --execute-top 1 --replace-open-orders --replace-price-threshold-cents 1.0
```

Sem `--replace-open-orders`, o bot bloqueia nova ordem quando já existe ordem aberta do mesmo lado para o mesmo `token_id`.

## Arquivos de exporto

- CSV: `export/paper_YYYYMMDD_HHMMSS...csv`
- JSON: opcional (via variável de ambiente interna)

## Parâmetros importantes

- Feed real Polymarket:
  - `--feed-mode polymarket`
  - `--polymarket-token-map BTCUSDC:token_id,ETHUSDC:token_id`
  - `--polymarket-clob-base-url`
  - `--polymarket-gamma-base-url`
  - `--polymarket-request-timeout`
  - Se estiver usando múltiplos mercados com mesmo ativo e horizonte (ex.: 5m/15m), é necessário usar os 4 símbolos reais de mercado do Polymarket no map (ex.: `BTC-5m-market`, `BTC-15m-market`, etc), não apenas `BTC5m`.
- Estratégia:
  - `--strategy-window`
  - `--strategy-threshold`
  - `--strategy-size-bps`
- Sinal em tempo real:
  - `python run_signal.py --product-id BTC-USD --horizons 5,15`
  - usa candles públicos de `Coinbase Exchange`
  - retorna `UP`, `DOWN` ou `NEUTRAL` com confiança
- Risco:
  - `--risk-max-trade`
  - `--risk-max-daily-loss`
  - `--risk-max-symbol`
  - `--risk-max-total`
  - `--risk-min-score`
  - `--risk-stop`
  - `--risk-take`
