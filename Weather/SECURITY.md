# Security Notes

## Segredos

- Nao commite `C:\Bot_poly\Weather\.env`.
- O projeto agora aceita carregar segredos fora da pasta do repo:
  - `WEATHER_ENV_PATH=C:\caminho\seguro\weather.env`
  - ou `WEATHER_SKIP_DOTENV=1` para usar apenas variaveis de ambiente do sistema.
- Para importar segredos para o perfil do Windows:

```powershell
powershell -ExecutionPolicy Bypass -File C:\Bot_poly\Weather\set_weather_secrets_windows.ps1 -EnvFile C:\segredos\weather.env
```

- Variaveis sensiveis principais:
  - `POLYMARKET_PRIVATE_KEY`
  - `POLYMARKET_API_KEY`
  - `POLYMARKET_API_SECRET`
  - `POLYMARKET_API_PASSPHRASE`
  - `POLYMARKET_FUNDER`

## Dados locais sensiveis

Mesmo sem gravar a chave privada, estes caminhos devem ser tratados como sensiveis:

- `C:\Bot_poly\Weather\export\db\weather_bot.db`
- `C:\Bot_poly\Weather\export\history\weather_model_scan_log.csv`
- `C:\Bot_poly\Weather\export\history\weather_model_latest.json`
- `C:\Bot_poly\Weather\export\state\*.lock`

Eles podem expor:

- oportunidades e estrategia
- carteira publica
- order ids e fills
- historico operacional

## Compartilhamento seguro

Para exportar relatorios sem campos sensiveis operacionais:

```bash
python run_weather_models.py --top 10 --safe-share --json
```

Ou:

```bash
python run_weather_models.py --top 10 --safe-share --export-json export/share/report.json
```

O modo `safe-share` remove ou reduz:

- `token_id`
- `market_id`
- `polymarket_url`
- ids operacionais de execucao
- `nonce`
- `submission_fingerprint`

## Dashboard

- Nao exponha o Streamlit publicamente sem autenticacao/reverse proxy.
- Lembre que o dashboard usa dados da carteira publica do Polymarket.

## Operacao recomendada

1. Use stake simbolico enquanto valida o bot.
2. Mantenha a pasta `export/` fora de sync em nuvem publica.
3. Revise periodicamente o historico de arquivos sensiveis antes de compartilhar o workspace.

## Launchers seguros

- Validacao local usando apenas ambiente do sistema:

```bat
C:\Bot_poly\Weather\run_validation_local_secure.bat
```

- Backup seguro do projeto:

```bash
python C:\Bot_poly\Weather\run_safe_backup.py
```
