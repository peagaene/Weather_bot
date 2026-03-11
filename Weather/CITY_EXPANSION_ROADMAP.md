# City Expansion Roadmap

## Current rollout state
- All current Polymarket weather cities are configured in observation.
- Non-US cities are `observation only` by default and are blocked from live by policy.
- US cities keep the full stack (`NWS`, `MOS`, `HRRR`, global models).
- International cities currently use the global forecast stack first.

## Active provider classes
- `open_meteo`
  - Role: baseline global forecast coverage
  - Status: active for every city
- `nws`
  - Role: US forecast + observation truth
  - Status: active only for US cities
- `mos`
  - Role: US station guidance
  - Status: active only for US cities
- `hrrr`
  - Role: short-horizon US model
  - Status: active only for US cities

## Candidate providers to add next
- `Tomorrow.io`
  - Reason: global forecast API, strong city coverage
- `WeatherAPI`
  - Reason: simple global forecast fallback
- `Visual Crossing`
  - Reason: global forecast + history in one provider
- `OpenWeather`
  - Reason: broad global availability and existing code path
- `Meteostat`
  - Reason: observation truth candidate for international cities
- `NOAA ISD`
  - Reason: raw station truth backfill candidate

## Rollout order
1. Keep new cities in observation only until history accumulates.
2. Add one new forecast provider class globally.
3. Add one international truth provider (`Meteostat` or `ISD`).
4. Accumulate source error metrics by `city/day_label`.
5. Enable live one city at a time when:
   - enough observations exist,
   - source agreement is stable,
   - policy recommendations stop returning `block`.

## Live enablement criteria
- Minimum observed history: 25+ resolved/observed daily highs for that city
- Stable source error profile: no dominant provider with large bias
- Market unit validated: `°F` or `°C` confirmed in stored opportunities
- Policy recommendation: `observe` or `allow`, not `block`
