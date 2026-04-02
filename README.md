# DayPulse Webhook Dashboard (Weather + Markets + Google Calendar)

<!--
DayPulse : plugin météo, marchés et agenda pour TRMNL
-->


This project (DayPulse) generates data (Open-Meteo + yfinance + Google Calendar) and sends it to a TRMNL Private Plugin using the TRMNL webhook endpoint.

It also supports generating a local preview HTML so you can see what will render on the device.

## 1) TRMNL setup

1. Create a **Private Plugin** in your TRMNL account.
2. In the plugin instance settings, choose data retrieval strategy **Webhook**.
3. Copy the **Webhook URL** (it contains your Plugin Settings UUID) into your config (`trmnl.webhook_url`).
4. Paste the markup from [daypulse_markup.liquid](daypulse_markup.liquid) into the plugin **Markup** editor.

## 2) Python setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 3) Configuration

Start from [config.example.yaml](config.example.yaml) and create your own `config.yaml`.

Translations live in [translations.yaml](translations.yaml). Set `general.language` to `fr` or `en`.

Optional additions in the YAML config:

- `general.network.request_timeout_seconds`, `max_retries`, `retry_delay_seconds`, `retry_backoff`, `retry_statuses`
- `trmnl.payload_soft_limit_bytes` to tune the payload-size warning threshold
- `finance.entries[]` to define each instrument with `symbol`, `label`, `currency`, and `show_currency`

The script now emits richer `INFO` logs by default: configuration summary, source start/end, source timings, fallback usage and human-readable TRMNL HTTP diagnostics.

### Google Calendar authentication

You can use either:

- **OAuth** (interactive):
  - Create OAuth Desktop credentials in Google Cloud.
  - Save as `credentials.json` next to the script.
  - First run will open a browser and create `token.json`.

- **Service account** (headless):
  - Create a service account JSON key.
  - Share your calendar with the service account email.

## 4) Run

Send data to TRMNL (default):


```powershell
python .\daypulse.py --config .\config.yaml
```

Preview only (no POST):


```powershell
python .\daypulse.py --config .\config.yaml --no-send --preview-html .\preview.html
```

Random test mode (no external API calls):


```powershell
# Reproducible random output (same seed => same screen)
python .\daypulse.py --config .\config.yaml --test-random --test-seed 123 --no-send --preview-html .\preview.html

# Increase or decrease simulated failures per block (0..1)
python .\daypulse.py --config .\config.yaml --test-random --test-failure-rate 0.5 --no-send --preview-html .\preview.html
```

Log the full webhook payload before preview/send:


```powershell
python .\daypulse.py --config .\config.yaml --log-payload --no-send
```


The preview renderer uses [preview_template.html](preview_template.html).

## Notes

- TRMNL webhook rate limits apply (see TRMNL docs). Keep payload small by limiting calendar events.
- Network calls now use configurable retries and backoff.
- `finance.entries` keeps ticker symbol, label and currency behavior together in one place.
- Set `show_currency: false` for instruments like the CAC 40 when you do not want a unit displayed.
- The script (DayPulse) logs the serialized webhook payload size and warns when it approaches or exceeds a practical soft limit of 2048 bytes.
- TRMNL responses are logged with both the raw HTTP code and a readable diagnostic message.
