# Commands for Loading Historical Options Data

## Load Historical Data for a Symbol

Run the following command from your project root to load historical options data for the symbol and date range specified in `config.yaml`:

```bash
python utilities/load_historical_options_data.py
```

- The script will read all parameters (symbol, date range, table ID, project ID) from `config.yaml`.
- To change the symbol, table, or date range, edit `config.yaml` and rerun the command.

## Example config.yaml

```yaml
project_id: voldilsloc
symbol: AAPL
table_id: historical_data.appl
date_start: 2025-01-01
```

---

For more advanced usage, you can import and call `fetch_historical_options` from another script or notebook.
