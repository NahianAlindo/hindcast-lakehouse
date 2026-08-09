# Measure additivity

Verbatim from `docs/PLAN.md` §6.6 -- also embedded as a dbt doc block
(`{% docs measure_additivity %}`, `warehouse/dbt/hindcast/models/marts/facts/_facts.yml`)
so it surfaces in `dbt docs generate`. Three of the seven rows are traps a
careless Power BI model would fall into.

| Measure | Additivity | Correct aggregation | Wrong aggregation to avoid |
|---|---|---|---|
| `temp_actual_c`, `temp_fcst_*` | **Non-additive** | AVG / MIN / MAX only | Never SUM |
| `precip_actual_mm` | **Fully additive** | SUM across time and location | Never AVG over a window |
| `pressure_hpa`, `humidity_pct` | **Semi-additive** | AVG over time; never SUM | SUM over time |
| `abs_err_temp_*` | Additive numerator | `AVG` → MAE | — |
| `sq_err_temp_*` | Additive numerator | `SQRT(AVG(sq_err))` → RMSE | **Never AVG of pre-computed RMSEs** |
| `brier_*` | Additive numerator | AVG of squared prob error | Never AVG of pre-computed Brier scores |
| `revision_count` | Additive | SUM / AVG | — |
| `wind_deg` | **Circular** | Vector mean, or circular error | Never arithmetic mean or plain difference |

## Why this matters for `fct_forecast_slot` / `fct_forecast_error`

- `temp_fcst_{L}h` and `temp_actual_c` are point-in-time readings, not
  accumulations. A Power BI card summing temperature across locations
  produces a number with no physical meaning.
- `abs_err_temp_{L}h` and `sq_err_temp_{L}h` exist specifically so MAE and
  RMSE can be computed correctly downstream: MAE is the average of the
  already-additive absolute errors; RMSE is the square root of the average
  squared error, **not** the average of per-row RMSEs (which isn't even a
  well-defined quantity for a single row).
- `brier_{L}h` is already the squared probability error for one row --
  average it across rows for the Brier score. Never average pre-aggregated
  Brier scores from different slices; recompute from the row-level values.
- `wind_dir_circ_err_{L}h` is pre-computed with the circular-distance
  formula (`docs/PLAN.md` §6.3b) precisely so BI models never need to
  subtract two wind directions directly -- 359° and 1° are 2° apart, not
  358°.
