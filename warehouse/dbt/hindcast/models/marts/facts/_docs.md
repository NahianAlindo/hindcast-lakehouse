{% docs measure_additivity %}
Full table: `bi/measures.md`. Summary: `temp_*` measures are non-additive
(AVG/MIN/MAX only, never SUM). `precip_*` measures are fully additive (SUM
is correct). `sq_err_temp_*`/`brier_*` are additive numerators -- aggregate
the row-level values (`SQRT(AVG(sq_err))` for RMSE), never average
pre-computed per-row scores. `wind_deg`/`wind_dir_circ_err_*` are circular
-- never a plain arithmetic difference.
{% enddocs %}
