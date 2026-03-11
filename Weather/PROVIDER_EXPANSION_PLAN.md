# Provider Expansion Plan

## Goal
Expand weather coverage safely without degrading live execution quality.

## Rules
- Add one provider at a time.
- Validate each provider in observation mode before using it for policy or live.
- Do not enable live for new cities until enough local history exists.

## Priority order

### Phase 1: Global observation truth
1. `Meteostat`
   - Purpose: international observed daily highs
   - Why first: unlocks truth/validation for non-US cities already in observation
   - Validation:
     - data fetched for at least 3 international cities
     - rows stored in `station_observation_daily_highs`
     - no regression in `run_weather_models.py`

2. `NOAA ISD`
   - Purpose: raw global station truth backfill
   - Why second: complements Meteostat and reduces single-source dependence

### Phase 2: New global forecast providers
3. `Weatherbit`
   - Purpose: forecast + current weather
   - Why: strong commercial global coverage

4. `Meteosource`
   - Purpose: global forecast and historical forecast
   - Why: useful for cities outside the US

5. `MET Norway`
   - Purpose: free global forecast
   - Why: broad coverage, good fallback

### Phase 3: City/region-specific reinforcement
6. `Bright Sky`
   - Purpose: Germany / DWD-backed support
   - Why: improves Munich specifically

7. `meteoblue`
   - Purpose: additional commercial forecast
   - Why: good for multi-model comparison

### Phase 4: Optional commercial/enterprise additions
8. `AccuWeather`
9. `Weatherstack`
10. `Meteomatics`
11. `Xweather`
12. `The Weather Company`

## Live policy during rollout
- Existing live bot can keep running.
- New provider integrations should be validated with manual scan runs first.
- Restart live only after:
  - focused tests pass
  - a dry scan completes
  - provider-specific storage rows are being written correctly
