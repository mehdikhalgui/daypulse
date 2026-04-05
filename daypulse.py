from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import random
import sys
import time
from logging.handlers import RotatingFileHandler
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, TypedDict

import requests
import yaml
import yfinance as yf
from geopy.geocoders import Nominatim


try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


LOGGER = logging.getLogger("daypulse")
T = TypeVar("T")
PREVIEW_DOCUMENT_TITLE = "Tableau de bord - Aperçu"


class NetworkConfig(TypedDict):
    """Configuration values used by retryable network operations."""

    request_timeout_seconds: int
    max_retries: int
    retry_delay_seconds: float
    retry_backoff: float
    retry_statuses: List[int]


class WeatherCurrent(TypedDict):
    """Current weather block sent to the widget."""

    time_label: str
    temperature: int | str
    temperature_display: str
    humidity: int | str
    wind_direction: str
    wind_speed: int | str
    wind_speed_unit: str
    wind_summary: str
    sunrise_time: str
    sunset_time: str
    moon_phase_icon: str
    icon: str


class WeatherForecastDay(TypedDict):
    """One forecast day entry for the weather widget."""

    day_label: str
    icon: str
    precip_probability: int | str
    temp_max: int | str
    temp_min: int | str


class WeatherPayload(TypedDict):
    """Complete weather payload consumed by preview and TRMNL markup."""

    ok: bool
    city: str
    current: WeatherCurrent
    forecast: List[WeatherForecastDay]


class FinanceIndex(TypedDict):
    """One finance row displayed in the markets widget."""

    symbol: str
    label: str
    price: str
    currency: Optional[str]
    show_currency: bool
    change_percent: str
    arrow: str


class FinancePayload(TypedDict):
    """Markets widget payload."""

    ok: bool
    indices: List[FinanceIndex]


class FinanceConfigEntry(TypedDict, total=False):
    """One finance configuration entry describing a displayed instrument."""

    symbol: str
    label: str
    currency: str
    show_currency: bool


class CalendarEvent(TypedDict):
    """One calendar event displayed inside a day column."""

    time_label: str
    summary: str


class CalendarDay(TypedDict):
    """One calendar day column in the 7-day layout."""

    date: str
    day_label: str
    is_weekend: bool
    events: List[CalendarEvent]


class CalendarPayload(TypedDict):
    """Agenda widget payload."""

    ok: bool
    days: List[CalendarDay]


class MetaPayload(TypedDict, total=False):
    """Metadata block useful for diagnostics and preview tooling."""

    lang: str
    generated_at: str
    errors: List[str]
    timings: Dict[str, float]
    test_mode: bool
    seed: Optional[int]
    failure_rate: float


class MergeVariables(TypedDict):
    """Top-level merge_variables object sent to TRMNL."""

    t: Dict[str, str]
    weather: WeatherPayload
    finance: FinancePayload
    calendar: CalendarPayload
    meta: MetaPayload


ICON_CODE_MAP = {
    "wi-day-sunny": 0,
    "wi-day-cloudy": 1,
    "wi-cloudy": 2,
    "wi-fog": 3,
    "wi-sprinkle": 4,
    "wi-rain": 5,
    "wi-snow": 6,
    "wi-thunderstorm": 7,
    "wi-night-clear": 8,
    "wi-night-alt-cloudy": 9,
    "wi-night-fog": 10,
    "wi-night-alt-sprinkle": 11,
    "wi-night-alt-rain": 12,
    "wi-night-alt-snow": 13,
    "wi-night-alt-thunderstorm": 14,
}


MOON_ICON_CODE_MAP = {
    "wi-moon-alt-new": 0,
    "wi-moon-alt-waxing-crescent-3": 1,
    "wi-moon-alt-first-quarter": 2,
    "wi-moon-alt-waxing-gibbous-3": 3,
    "wi-moon-alt-full": 4,
    "wi-moon-alt-waning-gibbous-3": 5,
    "wi-moon-alt-third-quarter": 6,
    "wi-moon-alt-waning-crescent-3": 7,
}


TRANSLATION_KEYS_FOR_TEMPLATE = (
    "data_unavailable",
    "humidity",
    "wind",
    "sunrise",
    "sunset",
    "forecast",
    "no_events",
)


TRANSLATION_KEY_MAP = {
    "data_unavailable": "du",
    "humidity": "h",
    "wind": "w",
    "sunrise": "sr",
    "sunset": "ss",
    "forecast": "fc",
    "no_events": "ne",
}


def _icon_code(icon_name: str) -> int:
    """Return the compact numeric code for a weather icon name."""
    return int(ICON_CODE_MAP.get(icon_name, ICON_CODE_MAP["wi-cloudy"]))


def _moon_icon_code(icon_name: str) -> int:
    """Return the compact numeric code for a moon-phase icon name."""
    return int(MOON_ICON_CODE_MAP.get(icon_name, MOON_ICON_CODE_MAP["wi-moon-alt-new"]))


def _compact_translations(translations: Dict[str, str]) -> Dict[str, str]:
    """Keep only the translation strings actually consumed by the Liquid template."""
    return {
        TRANSLATION_KEY_MAP[key]: str(translations.get(key, ""))
        for key in TRANSLATION_KEYS_FOR_TEMPLATE
        if key in TRANSLATION_KEY_MAP
    }


def _compact_merge_variables(merge_variables: MergeVariables) -> Dict[str, Any]:
    """Build the compact payload consumed by TRMNL and preview Liquid."""
    weather = merge_variables.get("weather", {})
    weather_current = weather.get("current", {})
    weather_forecast = list(weather.get("forecast") or [])
    finance = merge_variables.get("finance", {})
    finance_indices = list(finance.get("indices") or [])
    calendar = merge_variables.get("calendar", {})
    calendar_days = list(calendar.get("days") or [])

    return {
        "t": _compact_translations(merge_variables.get("t", {})),
        "w": {
            "o": bool(weather.get("ok")),
            "ci": str(weather.get("city", "")),
            "c": {
                "td": str(weather_current.get("temperature_display", "—")),
                "h": weather_current.get("humidity", "—"),
                "ws": weather_current.get("wind_speed", "—"),
                "wu": str(weather_current.get("wind_speed_unit", "")),
                "wd": str(weather_current.get("wind_direction", "—")),
                "sr": str(weather_current.get("sunrise_time", "—")),
                "ss": str(weather_current.get("sunset_time", "—")),
                "mi": _moon_icon_code(str(weather_current.get("moon_phase_icon", "wi-moon-alt-new"))),
                "i": _icon_code(str(weather_current.get("icon", "wi-cloudy"))),
            },
            "f": [
                {
                    "d": str(day.get("day_label", "—")),
                    "i": _icon_code(str(day.get("icon", "wi-cloudy"))),
                    "pp": day.get("precip_probability", "—"),
                    "x": day.get("temp_max", "—"),
                    "n": day.get("temp_min", "—"),
                }
                for day in weather_forecast
            ],
        },
        "f": {
            "o": bool(finance.get("ok")),
            "i": [
                {
                    "l": str(index.get("label", "")),
                    "p": str(index.get("price", "—")),
                    "c": str(index.get("change_percent", "—")),
                    "a": str(index.get("arrow", "•")),
                }
                for index in finance_indices
            ],
        },
        "c": {
            "o": bool(calendar.get("ok")),
            "d": [
                {
                    "l": str(day.get("day_label", "")),
                    "w": bool(day.get("is_weekend")),
                    "e": [
                        {
                            "t": str(event.get("time_label", "")),
                            "s": str(event.get("summary", "")),
                        }
                        for event in list(day.get("events") or [])
                    ],
                }
                for day in calendar_days
            ],
        },
    }


def _normalize_precip_probability(value: Any) -> int | str:
    """Hide very low precipitation probabilities and round visible values to 5% steps."""
    try:
        precip_value = float(value)
    except (TypeError, ValueError):
        return "—"

    if precip_value < 10:
        return "—"

    if precip_value > 90:
        return 100

    return int(((precip_value + 2.5) // 5) * 5)

TRMNL_STATUS_MESSAGES = {
    200: "OK - data accepted by TRMNL.",
    400: "Bad request - validate the webhook payload format.",
    401: "Unauthorized - webhook URL is invalid or missing.",
    404: "Plugin not found - verify the webhook URL.",
    422: "Unprocessable entity - payload structure is not accepted.",
    429: "Rate limited - reduce send frequency.",
    500: "Server error on TRMNL side.",
    502: "Bad gateway on TRMNL side.",
    503: "TRMNL service unavailable.",
    504: "TRMNL gateway timeout.",
}

TRMNL_PAYLOAD_SOFT_LIMIT_BYTES = 2048


def _get_trmnl_payload_soft_limit_bytes(config: Dict[str, Any]) -> int:
    """Return the configured TRMNL payload soft limit, or the default fallback."""
    trmnl_cfg = _as_dict(config.get("trmnl"))
    configured = trmnl_cfg.get("payload_soft_limit_bytes", TRMNL_PAYLOAD_SOFT_LIMIT_BYTES)
    value = TRMNL_PAYLOAD_SOFT_LIMIT_BYTES
    try:
        parsed_value = int(configured)
        if parsed_value > 0:
            value = parsed_value
    except (TypeError, ValueError):
        value = TRMNL_PAYLOAD_SOFT_LIMIT_BYTES
    return value


def _load_yaml(path: str) -> Dict[str, Any]:
    """Load a YAML file and enforce a mapping at the document root."""
    if not os.path.exists(path):
        cwd = os.getcwd()
        raise FileNotFoundError(
            f"File not found: {path} (cwd: {cwd}). "
            f"Create it from 'config.example.yaml' or pass --config .\\config.example.yaml."
        )
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return data


def _as_dict(value: Any) -> Dict[str, Any]:
    """Return a dictionary value or an empty mapping when the input is not a dict."""
    return value if isinstance(value, dict) else {}


def _resolve_path(path: str, *, base_dir: Optional[str] = None) -> str:
    """Resolve a possibly relative path, optionally against a specific base directory."""
    normalized_path = str(path).strip()
    if not normalized_path:
        return ""
    if os.path.isabs(normalized_path) or not base_dir:
        return normalized_path
    return os.path.normpath(os.path.join(base_dir, normalized_path))


def _get_general_paths_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Return the optional general.paths mapping from the config."""
    general_cfg = _as_dict(config.get("general"))
    return _as_dict(general_cfg.get("paths"))


def _get_translations_path(config: Dict[str, Any], cli_path: Optional[str], *, config_dir: str) -> str:
    """Resolve the translations YAML path from CLI override or config."""
    if cli_path:
        return _resolve_path(cli_path)

    paths_cfg = _get_general_paths_config(config)
    configured_path = str(paths_cfg.get("translations_yaml", "")).strip()
    if configured_path:
        return _resolve_path(configured_path, base_dir=config_dir)
    return _resolve_path("translations.yaml", base_dir=config_dir)


def _get_markup_path(config: Dict[str, Any], cli_path: Optional[str], *, config_dir: str, program_name: str) -> str:
    """Resolve the Liquid markup path from CLI override, config, or default script-based lookup."""
    if cli_path:
        return _resolve_path(cli_path)

    paths_cfg = _get_general_paths_config(config)
    configured_path = str(paths_cfg.get("markup_liquid", "")).strip()
    if configured_path:
        resolved_configured_path = _resolve_path(configured_path, base_dir=config_dir)
        if os.path.exists(resolved_configured_path):
            return resolved_configured_path
        raise FileNotFoundError(f"Configured Liquid markup file not found: {resolved_configured_path}")

    return _resolve_markup_path(program_name)


def _get_network_config(config: Dict[str, Any]) -> NetworkConfig:
    """Extract network retry and timeout settings from the global configuration."""
    general_cfg = _as_dict(config.get("general"))
    network_cfg = _as_dict(general_cfg.get("network"))
    retry_statuses = list(network_cfg.get("retry_statuses") or [429, 500, 502, 503, 504])
    return {
        "request_timeout_seconds": int(network_cfg.get("request_timeout_seconds", 15)),
        "max_retries": max(1, int(network_cfg.get("max_retries", 3))),
        "retry_delay_seconds": max(0.0, float(network_cfg.get("retry_delay_seconds", 1.5))),
        "retry_backoff": max(1.0, float(network_cfg.get("retry_backoff", 2.0))),
        "retry_statuses": [int(code) for code in retry_statuses],
    }


def _get_finance_entries(config: Dict[str, Any]) -> List[FinanceConfigEntry]:
    """Read finance instruments from config, supporting both new and legacy formats."""
    finance_cfg = _as_dict(config.get("finance"))
    entries: List[FinanceConfigEntry] = []
    if "entries" in finance_cfg:
        raw_entries = finance_cfg.get("entries")
        if not isinstance(raw_entries, list):
            LOGGER.warning("finance.entries must be a list; ignoring invalid value")
        else:
            for raw_entry in raw_entries:
                if not isinstance(raw_entry, dict):
                    continue
                symbol = str(raw_entry.get("symbol", "")).strip()
                if not symbol:
                    continue
                entry: FinanceConfigEntry = {"symbol": symbol}
                label = str(raw_entry.get("label", "")).strip()
                if label:
                    entry["label"] = label
                currency = str(raw_entry.get("currency", "")).strip().upper()
                if currency:
                    entry["currency"] = currency
                if "show_currency" in raw_entry:
                    entry["show_currency"] = bool(raw_entry.get("show_currency"))
                entries.append(entry)
    else:
        tickers = list(finance_cfg.get("tickers") or [])
        raw_label_map = _as_dict(finance_cfg.get("label_map"))
        raw_currency_map = _as_dict(finance_cfg.get("currency_map"))
        for raw_ticker in tickers:
            symbol = str(raw_ticker).strip()
            if not symbol:
                continue
            entry = {"symbol": symbol}
            label = str(raw_label_map.get(symbol, "")).strip()
            if label:
                entry["label"] = label
            currency = str(raw_currency_map.get(symbol, "")).strip().upper()
            if currency:
                entry["currency"] = currency
            entries.append(entry)
    return entries


def _retry_sleep_seconds(retry_delay_seconds: float, retry_backoff: float, attempt: int) -> float:
    """Compute the wait time before the next retry attempt."""
    return retry_delay_seconds * (retry_backoff ** max(0, attempt - 1))


def _request_with_retry(
    config: Dict[str, Any],
    method: str,
    url: str,
    *,
    operation: str,
    timeout_seconds: Optional[int] = None,
    retry_statuses: Optional[List[int]] = None,
    **kwargs: Any,
) -> requests.Response:
    """Execute an HTTP request with retry support for transient failures and statuses."""
    network_cfg = _get_network_config(config)
    effective_timeout = timeout_seconds or int(network_cfg["request_timeout_seconds"])
    max_retries = int(network_cfg["max_retries"])
    retry_delay_seconds = float(network_cfg["retry_delay_seconds"])
    retry_backoff = float(network_cfg["retry_backoff"])
    statuses = retry_statuses or list(network_cfg["retry_statuses"])

    last_exc: Optional[Exception] = None
    last_response: Optional[requests.Response] = None
    response_to_return: Optional[requests.Response] = None

    for attempt in range(1, max_retries + 1):
        LOGGER.info("%s (attempt %s/%s)", operation, attempt, max_retries)
        try:
            response = requests.request(method, url, timeout=effective_timeout, **kwargs)
            last_response = response
            if response.status_code in statuses and attempt < max_retries:
                wait_seconds = _retry_sleep_seconds(retry_delay_seconds, retry_backoff, attempt)
                LOGGER.warning(
                    "%s returned retryable HTTP %s; retrying in %.1fs",
                    operation,
                    response.status_code,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            last_exc = None
            response_to_return = response
            break
        except requests.RequestException as exc:
            LOGGER.warning("%s failed on attempt %s/%s: %s", operation, attempt, max_retries, exc)
            last_exc = exc
            if attempt >= max_retries:
                break
            wait_seconds = _retry_sleep_seconds(retry_delay_seconds, retry_backoff, attempt)
            LOGGER.info("Retrying %s in %.1fs", operation, wait_seconds)
            time.sleep(wait_seconds)

    if last_exc is not None:
        raise last_exc
    if last_response is None:
        raise RuntimeError(f"{operation} failed before receiving a response")
    if response_to_return is None:
        response_to_return = last_response
    return response_to_return


def _run_with_retries(
    operation: str,
    *,
    max_retries: int,
    retry_delay_seconds: float,
    retry_backoff: float,
    func: Callable[[int], T],
) -> T:
    """Run an arbitrary callable with retry/backoff logic and return its result."""
    last_exc: Optional[Exception] = None
    result: Optional[T] = None
    for attempt in range(1, max_retries + 1):
        try:
            result = func(attempt)
            break
        except Exception as exc:
            LOGGER.warning("%s failed on attempt %s/%s: %s", operation, attempt, max_retries, exc)
            last_exc = exc
            if attempt >= max_retries:
                break
            wait_seconds = _retry_sleep_seconds(retry_delay_seconds, retry_backoff, attempt)
            LOGGER.info("Retrying %s in %.1fs", operation, wait_seconds)
            time.sleep(wait_seconds)
    if last_exc is not None:
        raise last_exc
    if result is None:
        raise RuntimeError(f"{operation} failed without an exception")
    return result


def _resolve_translations(translations: Dict[str, Any], lang: str) -> Dict[str, str]:
    """Merge the selected language with English fallback strings."""
    en_t = translations.get("en") or {}
    if not isinstance(en_t, dict):
        en_t = {}
    selected = translations.get(lang) or en_t
    if not isinstance(selected, dict):
        selected = {}
    merged = {str(key): str(value) for key, value in {**en_t, **selected}.items()}
    LOGGER.info("Translations ready for language '%s' (%s keys)", lang, len(merged))
    return merged


def _resolve_finance_label(symbol: str, configured_label: Optional[str], info: Any) -> str:
    """Choose the display label for a ticker from config first, then provider metadata."""
    label = symbol
    if configured_label:
        label = configured_label
    elif isinstance(info, dict):
        for key in ("shortName", "longName", "symbol"):
            value = info.get(key)
            if value:
                label = str(value)
                break
    return label


def _resolve_finance_currency(symbol: str, configured_currency: Optional[str], info: Any) -> Optional[str]:
    """Choose the currency code for a ticker from config first, then provider metadata."""
    del symbol
    currency: Optional[str] = None
    if configured_currency:
        currency = configured_currency.upper()
    elif isinstance(info, dict):
        value = info.get("currency")
        if value:
            currency = str(value).upper()
    return currency


def _format_price_with_currency(value: float, currency: Optional[str], *, show_currency: Optional[bool] = None) -> str:
    """Format a quote value with a compact currency representation when available."""
    amount = f"{value:,.2f}"
    formatted_price = amount
    if show_currency is not False and currency:
        symbols = {
            "EUR": "€",
            "USD": "$",
            "GBP": "£",
            "JPY": "¥",
            "CNY": "¥",
        }
        symbol = symbols.get(currency)
        if currency in {"USD", "GBP", "JPY", "CNY"} and symbol:
            formatted_price = f"{symbol}{amount}"
        elif currency == "EUR" and symbol:
            formatted_price = f"{amount} {symbol}"
        else:
            formatted_price = f"{amount} {currency}"
    return formatted_price


def _resolve_show_currency(entry: FinanceConfigEntry, currency: Optional[str]) -> bool:
    """Decide whether the price formatter should display a currency marker."""
    show_currency = currency is not None
    if "show_currency" in entry:
        show_currency = bool(entry["show_currency"])
    return show_currency


def _run_source(
    name: str,
    fetcher: Callable[[], T],
    fallback_factory: Callable[[], T],
) -> Tuple[T, Optional[Exception], float]:
    """Execute one data-source fetch, log its timing, and apply fallback data on failure."""
    LOGGER.info("[%s] Start", name)
    started = time.perf_counter()
    error: Optional[Exception] = None
    try:
        result = fetcher()
    except Exception as exc:
        error = exc
        fallback = fallback_factory()
        result = fallback
    elapsed = time.perf_counter() - started
    if error is None:
        LOGGER.info("[%s] Success in %.2fs", name, elapsed)
    else:
        LOGGER.error("[%s] Failed in %.2fs: %s", name, elapsed, error)
        LOGGER.info("[%s] Fallback data applied", name)
    return result, error, elapsed


def _describe_trmnl_status(status_code: int) -> str:
    """Translate a TRMNL HTTP status code into a readable diagnostic message."""
    return TRMNL_STATUS_MESSAGES.get(status_code, f"Unexpected HTTP status {status_code}.")


def _build_trmnl_payload_json(merge_variables: Dict[str, Any]) -> str:
    """Serialize the TRMNL webhook body into compact JSON."""
    return json.dumps({"merge_variables": merge_variables}, ensure_ascii=False, separators=(",", ":"))


def _log_payload_size(payload_json: str, *, soft_limit_bytes: int = TRMNL_PAYLOAD_SOFT_LIMIT_BYTES) -> int:
    """Log payload size and warn when the practical soft limit is reached or exceeded."""
    payload_size = len(payload_json.encode("utf-8"))
    usage_ratio = payload_size / soft_limit_bytes if soft_limit_bytes > 0 else 0.0
    LOGGER.info(
        "TRMNL payload size: %s bytes (%.0f%% of soft limit %s bytes)",
        payload_size,
        usage_ratio * 100.0,
        soft_limit_bytes,
    )
    if payload_size > soft_limit_bytes:
        LOGGER.warning(
            "TRMNL payload exceeds the practical soft limit: %s > %s bytes",
            payload_size,
            soft_limit_bytes,
        )
    elif usage_ratio >= 0.9:
        LOGGER.warning(
            "TRMNL payload is close to the practical soft limit: %s/%s bytes",
            payload_size,
            soft_limit_bytes,
        )
    return payload_size


def _resolve_positive_int(value: Any, default: int, *, minimum: int = 1) -> int:
    """Parse a positive integer configuration value with a safe fallback."""
    try:
        parsed = int(value)
        if parsed >= minimum:
            return parsed
    except (TypeError, ValueError):
        pass
    return default


def _setup_logging(
    mode: str,
    level: str,
    log_file: Optional[str],
    *,
    max_bytes: int = 0,
    backup_count: int = 0,
) -> None:
    """Configure console and file logging according to the selected mode."""
    handlers: List[logging.Handler] = []
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)

    if mode in {"console", "both"}:
        handlers.append(logging.StreamHandler(sys.stdout))
    if mode in {"file", "both"}:
        if not log_file:
            raise ValueError("log.file must be set when log mode is file/both")
        log_dir = os.path.dirname(os.path.abspath(log_file))
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        if max_bytes > 0 and backup_count > 0:
            handlers.append(
                RotatingFileHandler(
                    log_file,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding="utf-8",
                )
            )
        else:
            handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    if mode == "none":
        logging.basicConfig(level=logging.CRITICAL)
        return

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )


def _get_timezone(tz_name: str) -> dt.tzinfo:
    """Resolve an IANA timezone name into a timezone object."""
    if ZoneInfo is None:
        raise RuntimeError("zoneinfo is not available on this Python")
    try:
        timezone = ZoneInfo(tz_name)
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"Invalid timezone: {tz_name}") from exc
    return timezone


def _weekday_label(date_obj: dt.date, lang: str) -> str:
    """Return a compact weekday label in the configured language."""
    # Abbreviations tuned for compact 7-column calendar.
    en = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    fr = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
    names = fr if lang.lower().startswith("fr") else en
    return names[date_obj.weekday()]


def _is_weekend(date_obj: dt.date) -> bool:
    """Identify whether the provided date falls on a weekend."""
    return date_obj.weekday() >= 5


def _wind_direction_cardinal(degrees: Optional[float]) -> str:
    """Convert wind direction in degrees to a cardinal label."""
    if degrees is None:
        return "—"
    dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = int((degrees % 360) / 45.0 + 0.5) % 8
    return dirs[idx]


def _resolve_weather_units(config: Dict[str, Any]) -> Dict[str, str]:
    """Resolve weather units from a simple unit system or legacy explicit values."""
    weather_cfg = _as_dict(config.get("weather"))
    unit_system = str(weather_cfg.get("unit_system", "")).strip().lower()

    if unit_system == "imperial":
        return {
            "unit_system": "imperial",
            "temperature_unit": "fahrenheit",
            "temperature_symbol": "F",
            "wind_speed_unit": "mph",
            "wind_speed_label": "mph",
        }
    if unit_system == "metric":
        return {
            "unit_system": "metric",
            "temperature_unit": "celsius",
            "temperature_symbol": "C",
            "wind_speed_unit": "kmh",
            "wind_speed_label": "km/h",
        }

    temperature_unit = str(weather_cfg.get("temperature_unit", "celsius")).strip().lower() or "celsius"
    wind_speed_unit = str(weather_cfg.get("wind_speed_unit", "kmh")).strip().lower() or "kmh"

    wind_speed_labels = {
        "kmh": "km/h",
        "mph": "mph",
        "ms": "m/s",
        "kn": "kn",
    }
    temperature_symbols = {
        "celsius": "C",
        "fahrenheit": "F",
    }

    return {
        "unit_system": "legacy",
        "temperature_unit": temperature_unit,
        "temperature_symbol": temperature_symbols.get(temperature_unit, "C"),
        "wind_speed_unit": wind_speed_unit,
        "wind_speed_label": wind_speed_labels.get(wind_speed_unit, wind_speed_unit),
    }


def _format_temperature_display(temperature: int | str, temperature_symbol: str) -> str:
    """Return a compact temperature string with the degree sign but without the unit letter."""
    if temperature == "—":
        return "—"
    return f"{temperature}°"


def _format_wind_summary(direction: str, wind_speed: int | str, wind_speed_label: str) -> str:
    """Return a compact wind label including direction and speed when available."""
    parts: List[str] = []
    if direction and direction != "—":
        parts.append(direction)
    if wind_speed != "—":
        parts.append(f"{wind_speed} {wind_speed_label}".strip())
    return " ".join(parts) if parts else "—"


def _format_iso_time_label(value: Any) -> str:
    """Format an ISO datetime string as a compact HH:MM label."""
    text = str(value or "").strip()
    if not text:
        return "—"
    if "T" in text:
        text = text.split("T", 1)[1]
    if len(text) >= 5:
        return text[:5]
    return text


def _moon_phase_icon_for_time(current_time: str) -> str:
    """Return a TRMNL moon-phase icon name for the provided local current-weather timestamp."""
    reference_new_moon = dt.datetime(2000, 1, 6, 18, 14)
    synodic_month_days = 29.53058867
    try:
        moment = dt.datetime.fromisoformat(current_time)
    except Exception:
        moment = dt.datetime.now()

    days_since_new_moon = (moment - reference_new_moon).total_seconds() / 86400.0
    lunar_age = days_since_new_moon % synodic_month_days
    phase_index = int(((lunar_age / synodic_month_days) * 8) + 0.5) % 8

    phase_icons = [
        "wi-moon-alt-new",
        "wi-moon-alt-waxing-crescent-3",
        "wi-moon-alt-first-quarter",
        "wi-moon-alt-waxing-gibbous-3",
        "wi-moon-alt-full",
        "wi-moon-alt-waning-gibbous-3",
        "wi-moon-alt-third-quarter",
        "wi-moon-alt-waning-crescent-3",
    ]
    return phase_icons[phase_index]


def _guess_city_from_address(address: str) -> str:
    """Guess a city-like label from a free-form postal address when no provider city is available."""
    parts = [part.strip() for part in str(address).split(",") if str(part).strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    candidate_parts = parts[:-1]
    for candidate in reversed(candidate_parts):
        has_digit = any(ch.isdigit() for ch in candidate)
        if has_digit or len(candidate) <= 2:
            continue
        return candidate
    return candidate_parts[-1] if candidate_parts else parts[0]


def _extract_city_from_nominatim(raw: Dict[str, Any], address_query: str) -> str:
    """Extract a compact city label from a Nominatim geocoding response."""
    address = _as_dict(raw.get("address"))
    for key in (
        "city",
        "town",
        "village",
        "municipality",
        "hamlet",
        "suburb",
        "county",
        "state_district",
        "state",
    ):
        value = str(address.get(key, "")).strip()
        if value:
            return value
    display_name = str(raw.get("display_name", "")).strip()
    if display_name:
        return _guess_city_from_address(display_name)
    return _guess_city_from_address(address_query)


def _resolve_weather_config_city(config: Dict[str, Any]) -> str:
    """Return the best display label available from configured weather location values."""
    weather_cfg = _as_dict(config.get("weather"))
    city = str(weather_cfg.get("city", "")).strip()
    if city:
        return city
    address = str(weather_cfg.get("address", "")).strip()
    if address:
        guessed_city = _guess_city_from_address(address)
        if guessed_city:
            return guessed_city
    return ""


def _resolve_weather_location(config: Dict[str, Any], lang: str) -> Tuple[float, float, str]:
    """Resolve weather coordinates from an address via Nominatim or legacy latitude/longitude config."""
    weather_cfg = _as_dict(config.get("weather"))
    address_query = str(weather_cfg.get("address", "")).strip()
    legacy_city = str(weather_cfg.get("city", "")).strip()
    lat = weather_cfg.get("latitude")
    lon = weather_cfg.get("longitude")

    if address_query:
        network_cfg = _get_network_config(config)
        geolocator = Nominatim(user_agent="daypulse/1.0")

        def _geocode(_: int) -> Any:
            return geolocator.geocode(
                address_query,
                exactly_one=True,
                addressdetails=True,
                language=lang,
                timeout=int(network_cfg["request_timeout_seconds"]),
            )

        location = _run_with_retries(
            f"Weather geocoding for {address_query}",
            max_retries=int(network_cfg["max_retries"]),
            retry_delay_seconds=float(network_cfg["retry_delay_seconds"]),
            retry_backoff=float(network_cfg["retry_backoff"]),
            func=_geocode,
        )
        if location is None:
            raise ValueError(f"Unable to geocode weather.address: {address_query}")

        resolved_lat = float(location.latitude)
        resolved_lon = float(location.longitude)
        raw = getattr(location, "raw", None)
        raw_dict = raw if isinstance(raw, dict) else {}
        city = _extract_city_from_nominatim(raw_dict, address_query)
        LOGGER.info(
            "Weather address resolved via Nominatim: %s -> city=%s lat=%s lon=%s",
            address_query,
            city or "-",
            resolved_lat,
            resolved_lon,
        )
        return resolved_lat, resolved_lon, city or legacy_city or _guess_city_from_address(address_query)

    if lat is None or lon is None:
        raise ValueError("weather.address or weather.latitude/weather.longitude are required")

    return float(lat), float(lon), legacy_city


def _open_meteo_icon(weather_code: Optional[int], *, is_day: Optional[bool] = True) -> str:
    """Map an Open-Meteo weather code to the TRMNL icon identifier."""
    # Map Open-Meteo weather codes to TRMNL Weather Icons (wi-*.svg).
    # Icons used here are those hosted by TRMNL.
    is_night = is_day is False
    if weather_code is None:
        return "wi-cloudy"
    if weather_code == 0:
        if is_night:
            return "wi-night-clear"
        return "wi-day-sunny"
    if weather_code in {1, 2}:
        if is_night:
            return "wi-night-alt-cloudy"
        return "wi-day-cloudy"
    if weather_code == 3:
        return "wi-cloudy"
    if weather_code in {45, 48}:
        if is_night:
            return "wi-night-fog"
        return "wi-fog"
    if weather_code in {51, 53, 55, 56, 57}:
        if is_night:
            return "wi-night-alt-sprinkle"
        return "wi-sprinkle"
    if weather_code in {61, 63, 65, 66, 67, 80, 81, 82}:
        if is_night:
            return "wi-night-alt-rain"
        return "wi-rain"
    if weather_code in {71, 73, 75, 77, 85, 86}:
        if is_night:
            return "wi-night-alt-snow"
        return "wi-snow"
    if weather_code in {95, 96, 99}:
        if is_night:
            return "wi-night-alt-thunderstorm"
        return "wi-thunderstorm"
    return "wi-cloudy"


def fetch_weather(config: Dict[str, Any], lang: str) -> WeatherPayload:
    """Fetch current weather and a three-day forecast from Open-Meteo."""
    lat, lon, city = _resolve_weather_location(config, lang)
    units = _resolve_weather_units(config)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "timezone": "auto",
        "temperature_unit": units["temperature_unit"],
        "wind_speed_unit": units["wind_speed_unit"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,weather_code,is_day",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max,sunrise,sunset",
        "forecast_days": 6,
    }

    LOGGER.info("Weather request prepared for %s (lat=%s, lon=%s)", city or "unknown city", lat, lon)
    resp = _request_with_retry(
        config,
        "GET",
        url,
        operation="Weather request to Open-Meteo",
        params=params,
    )
    resp.raise_for_status()
    payload = resp.json()

    if "current" not in payload or "daily" not in payload:
        LOGGER.warning("Weather response missing expected keys: %s", sorted(payload.keys()))

    current = payload.get("current", {})
    daily = payload.get("daily", {})

    # Open-Meteo returns ISO timestamps in local time when timezone=auto.
    current_time = str(current.get("time", ""))
    time_label = current_time.replace("T", " ")[:16] if current_time else ""

    temperature = current.get("temperature_2m")
    humidity = current.get("relative_humidity_2m")
    wind_speed = current.get("wind_speed_10m")
    wind_dir = current.get("wind_direction_10m")
    weather_code = current.get("weather_code")
    is_day_value = current.get("is_day")
    is_day = bool(int(is_day_value)) if is_day_value is not None else None
    temperature_value = int(round(float(temperature))) if temperature is not None else "—"
    humidity_value = int(round(float(humidity))) if humidity is not None else "—"
    wind_speed_value = int(round(float(wind_speed))) if wind_speed is not None else "—"
    wind_direction = _wind_direction_cardinal(float(wind_dir)) if wind_dir is not None else "—"

    times: List[str] = list(daily.get("time") or [])
    wcodes: List[Any] = list(daily.get("weather_code") or [])
    tmax: List[Any] = list(daily.get("temperature_2m_max") or [])
    tmin: List[Any] = list(daily.get("temperature_2m_min") or [])
    precip_probability_max: List[Any] = list(daily.get("precipitation_probability_max") or [])
    sunrises: List[Any] = list(daily.get("sunrise") or [])
    sunsets: List[Any] = list(daily.get("sunset") or [])
    sunrise_time = _format_iso_time_label(sunrises[0]) if sunrises else "—"
    sunset_time = _format_iso_time_label(sunsets[0]) if sunsets else "—"
    moon_phase_icon = _moon_phase_icon_for_time(current_time)

    forecast: List[WeatherForecastDay] = []
    # Next 5 days (excluding today) if available.
    for i in range(1, min(6, len(times))):
        day_iso = times[i]
        try:
            day_date = dt.date.fromisoformat(day_iso)
        except Exception:
            LOGGER.debug("Weather forecast day ignored due to invalid date: %s", day_iso)
            continue

        day_label = _weekday_label(day_date, lang)
        forecast.append(
            {
                "day_label": day_label,
                "icon": _open_meteo_icon(int(wcodes[i]) if wcodes[i] is not None else None),
                "precip_probability": _normalize_precip_probability(
                    precip_probability_max[i] if i < len(precip_probability_max) else None
                ),
                "temp_max": int(round(float(tmax[i]))) if tmax[i] is not None else "—",
                "temp_min": int(round(float(tmin[i]))) if tmin[i] is not None else "—",
            }
        )

    LOGGER.info(
        "Weather OK - %s, temp=%s%s, humidity=%s, wind=%s, forecast_days=%s",
        city or "unknown city",
        temperature_value,
        f"°{units['temperature_symbol']}" if temperature_value != "—" else "",
        humidity_value,
        _format_wind_summary(wind_direction, wind_speed_value, units["wind_speed_label"]),
        len(forecast),
    )

    return {
        "ok": True,
        "city": city,
        "current": {
            "time_label": time_label,
            "temperature": temperature_value,
            "temperature_display": _format_temperature_display(temperature_value, units["temperature_symbol"]),
            "humidity": humidity_value,
            "wind_direction": wind_direction,
            "wind_speed": wind_speed_value,
            "wind_speed_unit": units["wind_speed_label"],
            "wind_summary": _format_wind_summary(wind_direction, wind_speed_value, units["wind_speed_label"]),
            "sunrise_time": sunrise_time,
            "sunset_time": sunset_time,
            "moon_phase_icon": moon_phase_icon,
            "icon": _open_meteo_icon(int(weather_code) if weather_code is not None else None, is_day=is_day),
        },
        "forecast": forecast,
    }


def fetch_finance(config: Dict[str, Any]) -> FinancePayload:
    """Fetch quote snapshots for configured tickers using yfinance."""
    entries = _get_finance_entries(config)
    network_cfg = _get_network_config(config)

    indices: List[FinanceIndex] = []
    if not entries:
        LOGGER.info("Finance disabled effectively: no finance entries configured")
    else:
        LOGGER.info("Fetching finance quotes via yfinance for %s ticker(s)", len(entries))

    any_success = False
    success_count = 0
    if entries:
        for entry in entries:
            symbol = entry["symbol"]
            configured_label = entry.get("label")
            configured_currency = entry.get("currency")
            try:
                LOGGER.info("[finance] Fetching %s", symbol)

                def _load_symbol(_: int) -> Tuple[Any, Any, Optional[Dict[str, Any]]]:
                    """Load history and metadata for one ticker inside the retry wrapper."""
                    ticker = yf.Ticker(symbol)
                    history = ticker.history(period="1d", interval="5m")
                    if history is None or history.empty:
                        LOGGER.debug("[finance] %s has no intraday data, falling back to daily history", symbol)
                        history = ticker.history(period="5d", interval="1d")
                    if history is None or history.empty:
                        raise RuntimeError("No market data returned")
                    info = None
                    if configured_label is None or configured_currency is None:
                        try:
                            info = getattr(ticker, "info", None)
                        except Exception as info_exc:
                            LOGGER.debug("[finance] %s info lookup failed: %s", symbol, info_exc)
                    return ticker, history, info

                _, hist, info = _run_with_retries(
                    f"Finance request for {symbol}",
                    max_retries=int(network_cfg["max_retries"]),
                    retry_delay_seconds=float(network_cfg["retry_delay_seconds"]),
                    retry_backoff=float(network_cfg["retry_backoff"]),
                    func=_load_symbol,
                )

                open_price = float(hist["Open"].iloc[0]) if "Open" in hist.columns else float(hist["Close"].iloc[0])
                last_price = float(hist["Close"].iloc[-1]) if "Close" in hist.columns else float(hist.iloc[-1][0])
                change = (last_price - open_price) / open_price * 100.0 if open_price else 0.0
                arrow = "▲" if change >= 0 else "▼"
                label = _resolve_finance_label(symbol, configured_label, info)
                currency = _resolve_finance_currency(symbol, configured_currency, info)
                show_currency = _resolve_show_currency(entry, currency)
                formatted_price = _format_price_with_currency(last_price, currency, show_currency=show_currency)

                indices.append(
                    {
                        "symbol": symbol,
                        "label": label,
                        "price": formatted_price,
                        "currency": currency,
                        "show_currency": show_currency,
                        "change_percent": f"{change:+.2f}",
                        "arrow": arrow,
                    }
                )
                any_success = True
                success_count += 1
                LOGGER.info(
                    "[finance] %s OK - label=%s price=%s change=%s%%",
                    symbol,
                    label,
                    formatted_price,
                    f"{change:+.2f}",
                )
            except Exception as exc:
                LOGGER.warning("[finance] %s failed: %s", symbol, exc)
                indices.append(
                    {
                        "symbol": symbol,
                        "label": configured_label or symbol,
                        "price": "—",
                        "currency": configured_currency,
                        "show_currency": _resolve_show_currency(entry, configured_currency),
                        "change_percent": "—",
                        "arrow": "•",
                    }
                )

    LOGGER.info("Finance OK - %s/%s ticker(s) updated", success_count, len(entries))
    return {"ok": any_success, "indices": indices}


def _build_calendar_service_oauth(credentials_json: str, token_json: str) -> Any:
    """Build a Google Calendar client using OAuth desktop credentials."""
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/calendar.readonly"]
    creds = None

    if os.path.exists(token_json):
        creds = Credentials.from_authorized_user_file(token_json, scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_json, scopes)
            creds = flow.run_local_server(port=0)
        with open(token_json, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("calendar", "v3", credentials=creds)


def _build_calendar_service_service_account(service_account_json: str) -> Any:
    """Build a Google Calendar client using a service account."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/calendar.readonly"]
    creds = service_account.Credentials.from_service_account_file(service_account_json, scopes=scopes)
    return build("calendar", "v3", credentials=creds)


def _build_calendar_service_from_config(config: Dict[str, Any]) -> Any:
    """Build a Google Calendar client from the current configuration."""
    gcfg = _as_dict(config.get("google_calendar"))
    mode = str(gcfg.get("mode", "oauth")).strip().lower()

    if mode == "service_account":
        service_account_json = str(_as_dict(gcfg.get("service_account")).get("json", "")).strip()
        if not service_account_json:
            raise ValueError("google_calendar.service_account.json is required for service_account mode")
        return _build_calendar_service_service_account(service_account_json)

    credentials_json = str(_as_dict(gcfg.get("oauth")).get("credentials_json", "")).strip()
    token_json = str(_as_dict(gcfg.get("oauth")).get("token_json", "token.json")).strip()
    if not credentials_json:
        raise ValueError("google_calendar.oauth.credentials_json is required for oauth mode")
    return _build_calendar_service_oauth(credentials_json, token_json)


def list_google_calendars(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """List accessible Google Calendars for the configured account."""
    service = _build_calendar_service_from_config(config)
    calendars: List[Dict[str, Any]] = []
    page_token: Optional[str] = None

    while True:
        response = service.calendarList().list(pageToken=page_token).execute()
        for item in response.get("items", []):
            calendars.append(
                {
                    "id": str(item.get("id", "")),
                    "summary": str(item.get("summary", "")),
                    "accessRole": str(item.get("accessRole", "")),
                    "selected": bool(item.get("selected", False)),
                    "timeZone": str(item.get("timeZone", "")),
                    "primary": bool(item.get("primary", False)),
                }
            )
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return calendars


def _get_configured_calendar_ids(config: Dict[str, Any]) -> List[str]:
    """Return the configured Google Calendar ids, supporting both single and multiple forms."""
    gcfg = _as_dict(config.get("google_calendar"))
    configured_ids = gcfg.get("calendar_ids")

    if isinstance(configured_ids, list):
        calendar_ids = [str(value).strip() for value in configured_ids if str(value).strip()]
        if calendar_ids:
            return calendar_ids
    elif configured_ids is not None:
        configured_id = str(configured_ids).strip()
        if configured_id:
            return [configured_id]

    legacy_calendar_id = str(gcfg.get("calendar_id", "primary")).strip()
    return [legacy_calendar_id or "primary"]


def fetch_calendar(config: Dict[str, Any], lang: str, *, all_day_label: str) -> CalendarPayload:
    """Fetch calendar events for the next seven days and bucket them by day."""
    gcfg = config.get("google_calendar", {})
    ccfg = config.get("calendar", {})

    mode = str(gcfg.get("mode", "oauth")).strip().lower()
    tz_name = str(gcfg.get("timezone", "UTC"))
    tz = _get_timezone(tz_name)
    calendar_ids = _get_configured_calendar_ids(config)
    max_events_per_day = int(ccfg.get("max_events_per_day", 3))

    LOGGER.info(
        "Fetching Google Calendar events (%s, calendars=%s, timezone=%s)",
        mode,
        ", ".join(calendar_ids),
        tz_name,
    )
    service = _build_calendar_service_from_config(config)

    now = dt.datetime.now(tz=tz)
    time_min = now.isoformat()
    time_max = (now + dt.timedelta(days=7)).isoformat()

    events: List[Tuple[int, int, Dict[str, Any]]] = []
    for calendar_rank, calendar_id in enumerate(calendar_ids):
        events_result = (
            service.events()
            .list(
                calendarId=calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime",
                maxResults=250,
            )
            .execute()
        )
        calendar_events = events_result.get("items", [])
        LOGGER.info("[calendar] %s -> %s event(s) returned", calendar_id, len(calendar_events))
        for event_index, calendar_event in enumerate(calendar_events):
            events.append((calendar_rank, event_index, calendar_event))

    LOGGER.info(
        "Calendar API returned %s event(s) across %s calendar(s) for the next 7 days",
        len(events),
        len(calendar_ids),
    )

    days: List[CalendarDay] = []
    buckets: Dict[dt.date, List[Tuple[int, int, int, dt.datetime, str, CalendarEvent]]] = {}
    for i in range(7):
        d = (now.date() + dt.timedelta(days=i))
        buckets[d] = []

    dropped_events = 0
    for calendar_rank, event_index, ev in events:
        summary = str(ev.get("summary", ""))
        start = ev.get("start", {})
        start_dt: Optional[dt.datetime] = None
        start_date: Optional[dt.date] = None
        time_label = ""
        is_all_day = False

        if "dateTime" in start:
            # RFC3339; keep simple by parsing with fromisoformat after normalization.
            raw = str(start["dateTime"])
            try:
                # Python needs +00:00 not Z.
                raw2 = raw.replace("Z", "+00:00")
                start_dt = dt.datetime.fromisoformat(raw2)
                start_dt = start_dt.astimezone(tz)
                start_date = start_dt.date()
                time_label = start_dt.strftime("%H:%M")
            except Exception:
                LOGGER.debug("[calendar] Event ignored due to invalid dateTime: %s", raw)
                dropped_events += 1
                continue
        elif "date" in start:
            try:
                start_date = dt.date.fromisoformat(str(start["date"]))
                time_label = ""
                is_all_day = True
                start_dt = dt.datetime.combine(start_date, dt.time.min, tzinfo=tz)
            except Exception:
                LOGGER.debug("[calendar] Event ignored due to invalid all-day date: %s", start.get("date"))
                dropped_events += 1
                continue

        if not start_date or start_date not in buckets:
            LOGGER.debug("[calendar] Event dropped outside displayed range: %s (%s)", summary or "(untitled)", start_date)
            dropped_events += 1
            continue

        event_payload = {
            "time_label": time_label,
            "summary": summary,
        }
        buckets[start_date].append(
            (
                calendar_rank,
                0 if is_all_day else 1,
                event_index,
                start_dt or dt.datetime.combine(start_date, dt.time.min, tzinfo=tz),
                summary,
                event_payload,
            )
        )
        LOGGER.debug("[calendar] Event bucketed on %s: %s", start_date.isoformat(), summary or "(untitled)")

    for i in range(7):
        d = now.date() + dt.timedelta(days=i)
        day_label = f"{_weekday_label(d, lang)} {d.day:02d}"
        sorted_day_events = sorted(
            buckets.get(d, []),
            key=lambda item: (item[1], item[0], item[2]) if item[1] == 0 else (item[1], item[3], item[2], item[0], item[4]),
        )
        day_events = [item[5] for item in sorted_day_events[: max(0, max_events_per_day)]]
        LOGGER.info("[calendar] %s -> %s event(s)", day_label, len(day_events))
        days.append(
            {
                "date": d.isoformat(),
                "day_label": day_label,
                "is_weekend": _is_weekend(d),
                "events": day_events,
            }
        )

    total_bucketed = sum(len(items) for items in buckets.values())
    LOGGER.info(
        "Calendar OK - %s event(s) in range across %s calendar(s), %s dropped, max/day=%s",
        total_bucketed,
        len(calendar_ids),
        dropped_events,
        max_events_per_day,
    )
    return {"ok": True, "days": days}


def _default_weather(config: Dict[str, Any]) -> WeatherPayload:
    """Return a safe fallback weather payload that preserves the widget layout."""
    city = _resolve_weather_config_city(config)
    units = _resolve_weather_units(config)
    return {
        "ok": False,
        "city": city,
        "current": {
            "time_label": "—",
            "temperature": "—",
            "temperature_display": "—",
            "humidity": "—",
            "wind_direction": "—",
            "wind_speed": "—",
            "wind_speed_unit": units["wind_speed_label"],
            "wind_summary": "—",
            "sunrise_time": "—",
            "sunset_time": "—",
            "moon_phase_icon": "wi-moon-alt-new",
            "icon": "wi-cloudy",
        },
        "forecast": [
            {"day_label": "—", "icon": "wi-cloudy", "precip_probability": "—", "temp_max": "—", "temp_min": "—"},
            {"day_label": "—", "icon": "wi-cloudy", "precip_probability": "—", "temp_max": "—", "temp_min": "—"},
            {"day_label": "—", "icon": "wi-cloudy", "precip_probability": "—", "temp_max": "—", "temp_min": "—"},
            {"day_label": "—", "icon": "wi-cloudy", "precip_probability": "—", "temp_max": "—", "temp_min": "—"},
            {"day_label": "—", "icon": "wi-cloudy", "precip_probability": "—", "temp_max": "—", "temp_min": "—"},
        ],
    }


def _default_finance(config: Dict[str, Any]) -> FinancePayload:
    """Return a safe fallback finance payload for all configured tickers."""
    entries = _get_finance_entries(config)
    return {
        "ok": False,
        "indices": [
            {
                "symbol": entry["symbol"],
                "label": entry.get("label", entry["symbol"]),
                "price": "—",
                "currency": entry.get("currency"),
                "show_currency": _resolve_show_currency(entry, entry.get("currency")),
                "change_percent": "—",
                "arrow": "•",
            }
            for entry in entries
        ],
    }


def _default_calendar(config: Dict[str, Any], lang: str) -> CalendarPayload:
    """Return a seven-day empty calendar structure when live data is unavailable."""
    # Always produce 7 columns.
    gcfg = config.get("google_calendar", {})
    tz_name = str(gcfg.get("timezone", ""))
    if tz_name:
        try:
            today = dt.datetime.now(tz=_get_timezone(tz_name)).date()
        except Exception:
            today = dt.date.today()
    else:
        today = dt.date.today()
    days: List[CalendarDay] = []
    for i in range(7):
        d = today + dt.timedelta(days=i)
        day_label = f"{_weekday_label(d, lang)} {d.day:02d}"
        days.append(
            {
                "date": d.isoformat(),
                "day_label": day_label,
                "is_weekend": _is_weekend(d),
                "events": [],
            }
        )
    return {"ok": False, "days": days}


def build_merge_variables_random(
    config: Dict[str, Any],
    translations: Dict[str, Any],
    *,
    seed: Optional[int],
    failure_rate: float,
) -> MergeVariables:
    """Generate deterministic demo data and simulated failures for local testing."""
    lang = str((config.get("general") or {}).get("language", "en")).strip().lower() or "en"
    t = _resolve_translations(translations, lang)
    entries = _get_finance_entries(config)

    rng = random.Random(seed)
    failure_rate = max(0.0, min(1.0, float(failure_rate)))
    LOGGER.info("Random test mode enabled (seed=%s, failure_rate=%.2f)", seed, failure_rate)

    errors: List[str] = []

    # Weather
    if rng.random() < failure_rate:
        weather = _default_weather(config)
        errors.append("weather")
        LOGGER.warning("[weather] Random mode forced fallback data")
    else:
        units = _resolve_weather_units(config)
        city = _resolve_weather_config_city(config)
        temp = rng.randint(-5, 35)
        humidity = rng.randint(25, 95)
        wind_speed = rng.randint(5, 45)
        wind_deg = rng.uniform(0, 359.9)
        is_day = bool(rng.randint(0, 1))
        sunrise_time = f"{rng.randint(5, 7):02d}:{rng.choice([0, 10, 20, 30, 40, 50]):02d}"
        sunset_time = f"{rng.randint(18, 21):02d}:{rng.choice([0, 10, 20, 30, 40, 50]):02d}"
        icon_choices = [
            "wi-day-sunny",
            "wi-day-cloudy",
            "wi-cloudy",
            "wi-fog",
            "wi-sprinkle",
            "wi-rain",
            "wi-snow",
            "wi-thunderstorm",
            "wi-night-clear",
            "wi-night-alt-cloudy",
            "wi-night-fog",
            "wi-night-alt-sprinkle",
            "wi-night-alt-rain",
            "wi-night-alt-snow",
            "wi-night-alt-thunderstorm",
        ]
        icon = _open_meteo_icon(rng.choice([0, 1, 2, 3, 45, 61, 71, 95]), is_day=is_day)

        # Local-ish timestamp
        time_label = dt.datetime.now().strftime("%Y-%m-%d %H:%M")

        today = dt.date.today()
        forecast: List[WeatherForecastDay] = []
        for i in range(1, 6):
            d = today + dt.timedelta(days=i)
            tmax = rng.randint(temp, temp + 8)
            tmin = rng.randint(temp - 8, temp)
            forecast.append(
                {
                    "day_label": _weekday_label(d, lang),
                    "icon": rng.choice(icon_choices),
                    "precip_probability": _normalize_precip_probability(rng.randint(0, 100)),
                    "temp_max": tmax,
                    "temp_min": tmin,
                }
            )

        weather = {
            "ok": True,
            "city": city,
            "current": {
                "time_label": time_label,
                "temperature": temp,
                "temperature_display": _format_temperature_display(temp, units["temperature_symbol"]),
                "humidity": humidity,
                "wind_direction": _wind_direction_cardinal(wind_deg),
                "wind_speed": wind_speed,
                "wind_speed_unit": units["wind_speed_label"],
                "wind_summary": _format_wind_summary(
                    _wind_direction_cardinal(wind_deg),
                    wind_speed,
                    units["wind_speed_label"],
                ),
                "sunrise_time": sunrise_time,
                "sunset_time": sunset_time,
                "moon_phase_icon": _moon_phase_icon_for_time(time_label.replace(" ", "T")),
                "icon": icon,
            },
            "forecast": forecast,
        }
        LOGGER.info("[weather] Random data generated for %s", city or "unknown city")

    # Finance (max 3 indices in test mode)
    if not entries:
        entries = [
            {"symbol": "^FCHI", "label": "CAC 40", "show_currency": False},
            {"symbol": "XAUEUR=X", "label": "Or", "currency": "EUR", "show_currency": True},
            {"symbol": "AI.PA", "label": "Air Liquide", "currency": "EUR", "show_currency": True},
        ]
    test_entries = entries[:3]

    if rng.random() < failure_rate:
        finance = {
            "ok": False,
            "indices": [
                {
                    "symbol": entry["symbol"],
                    "label": entry.get("label", entry["symbol"]),
                    "price": "—",
                    "currency": entry.get("currency"),
                    "show_currency": _resolve_show_currency(entry, entry.get("currency")),
                    "change_percent": "—",
                    "arrow": "•",
                }
                for entry in test_entries
            ],
        }
        errors.append("finance")
        LOGGER.warning("[finance] Random mode forced fallback data")
    else:
        indices: List[FinanceIndex] = []
        for entry in test_entries:
            symbol = entry["symbol"]
            base = rng.uniform(1000.0, 50000.0)
            change = rng.uniform(-3.5, 3.5)
            price = base * (1.0 + change / 100.0)
            arrow = "▲" if change >= 0 else "▼"
            currency = entry.get("currency")
            show_currency = _resolve_show_currency(entry, currency)
            indices.append(
                {
                    "symbol": symbol,
                    "label": entry.get("label", symbol),
                    "price": _format_price_with_currency(price, currency, show_currency=show_currency),
                    "currency": currency,
                    "show_currency": show_currency,
                    "change_percent": f"{change:+.2f}",
                    "arrow": arrow,
                }
            )
        finance = {"ok": True, "indices": indices}
        LOGGER.info("[finance] Random data generated for %s ticker(s)", len(indices))

    # Calendar
    if rng.random() < failure_rate:
        calendar = _default_calendar(config, lang)
        errors.append("calendar")
        LOGGER.warning("[calendar] Random mode forced fallback data")
    else:
        gcfg = config.get("google_calendar", {})
        tz_name = str(gcfg.get("timezone", ""))
        if tz_name:
            try:
                today = dt.datetime.now(tz=_get_timezone(tz_name)).date()
            except Exception:
                today = dt.date.today()
        else:
            today = dt.date.today()

        ccfg = config.get("calendar", {})
        max_events_per_day = int(ccfg.get("max_events_per_day", 3))

        event_titles = [
            "Standup",
            "Project sync",
            "1:1",
            "Deep work",
            "Workout",
            "Dentist",
            "School",
            "Travel",
            "Family",
        ]

        days: List[CalendarDay] = []
        for i in range(7):
            d = today + dt.timedelta(days=i)
            day_label = f"{_weekday_label(d, lang)} {d.day:02d}"
            evs: List[CalendarEvent] = []

            # Some days have no events.
            count = rng.randint(0, max(0, max_events_per_day))
            for _ in range(count):
                is_all_day = rng.random() < 0.15
                if is_all_day:
                    time_label = ""
                else:
                    hour = rng.randint(7, 20)
                    minute = rng.choice([0, 15, 30, 45])
                    time_label = f"{hour:02d}:{minute:02d}"
                title = rng.choice(event_titles)
                evs.append(
                    {
                        "time_label": time_label,
                        "summary": title,
                    }
                )

            days.append(
                {
                    "date": d.isoformat(),
                    "day_label": day_label,
                    "is_weekend": _is_weekend(d),
                    "events": evs,
                }
            )

        calendar = {"ok": True, "days": days}
        LOGGER.info("[calendar] Random data generated for 7 day(s)")

    return {
        "t": t,
        "weather": weather,
        "finance": finance,
        "calendar": calendar,
        "meta": {
            "lang": lang,
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "errors": errors,
            "test_mode": True,
            "seed": seed,
            "failure_rate": failure_rate,
        },
    }


def build_merge_variables(config: Dict[str, Any], translations: Dict[str, Any]) -> MergeVariables:
    """Build the final merge_variables payload from live data sources."""
    lang = str((config.get("general") or {}).get("language", "en")).strip().lower() or "en"
    t = _resolve_translations(translations, lang)

    errors: List[str] = []
    timings: Dict[str, float] = {}

    weather, weather_exc, weather_elapsed = _run_source(
        "weather",
        lambda: fetch_weather(config, lang),
        lambda: _default_weather(config),
    )
    timings["weather"] = weather_elapsed
    if weather_exc is not None:
        errors.append("weather")

    finance, finance_exc, finance_elapsed = _run_source(
        "finance",
        lambda: fetch_finance(config),
        lambda: _default_finance(config),
    )
    timings["finance"] = finance_elapsed
    if finance_exc is not None:
        errors.append("finance")

    calendar, calendar_exc, calendar_elapsed = _run_source(
        "calendar",
        lambda: fetch_calendar(config, lang, all_day_label=str(t.get("all_day", "All day"))),
        lambda: _default_calendar(config, lang),
    )
    timings["calendar"] = calendar_elapsed
    if calendar_exc is not None:
        errors.append("calendar")

    LOGGER.info(
        "Source timings - weather=%.2fs finance=%.2fs calendar=%.2fs",
        timings["weather"],
        timings["finance"],
        timings["calendar"],
    )

    return {
        "t": t,
        "weather": weather,
        "finance": finance,
        "calendar": calendar,
        "meta": {
            "lang": lang,
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "errors": errors,
            "timings": timings,
        },
    }


def send_to_trmnl(
    config: Dict[str, Any],
    webhook_url: str,
    merge_variables: Dict[str, Any],
    timeout_seconds: int,
) -> requests.Response:
    """Send the webhook payload to TRMNL using the configured retry policy."""
    payload_json = _build_trmnl_payload_json(merge_variables)
    payload_soft_limit_bytes = _get_trmnl_payload_soft_limit_bytes(config)
    LOGGER.info(
        "Sending payload to TRMNL (timeout=%ss, size=%s bytes)",
        timeout_seconds,
        len(payload_json.encode("utf-8")),
    )
    _log_payload_size(payload_json, soft_limit_bytes=payload_soft_limit_bytes)
    resp = _request_with_retry(
        config,
        "POST",
        webhook_url,
        operation="TRMNL webhook POST",
        timeout_seconds=timeout_seconds,
        headers={"Content-Type": "application/json"},
        data=payload_json,
    )
    return resp


def _resolve_program_name(argv: Optional[List[str]] = None) -> str:
    """Return the invoked script name for help text and template resolution."""
    if argv:
        first_arg = str(argv[0]).strip()
        if first_arg and not first_arg.startswith("-"):
            return os.path.basename(first_arg)
    program_name = os.path.basename(sys.argv[0])
    return program_name or "daypulse.py"


def _resolve_markup_path(program_name: str) -> str:
    """Resolve the Liquid markup path from the current script name."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidate_names = [_resolve_primary_markup_name(program_name), "daypulse_markup.liquid"]
    for candidate_name in candidate_names:
        candidate_path = os.path.join(script_dir, candidate_name)
        if os.path.exists(candidate_path):
            return candidate_path
    raise FileNotFoundError(
        "No Liquid markup file found. Expected one of: " + ", ".join(candidate_names)
    )


def _resolve_primary_markup_name(program_name: str) -> str:
    """Return the markup filename that matches the current script stem."""
    script_stem, _ = os.path.splitext(os.path.basename(program_name))
    return f"{script_stem}_markup.liquid"


def _build_preview_document(rendered_markup: str) -> str:
    """Wrap rendered plugin markup in a standalone HTML preview document."""
    return f"""<!DOCTYPE html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <base href=\"https://trmnl.com/\" />
    <link rel=\"stylesheet\" href=\"https://trmnl.com/css/latest/plugins.css\" />
    <script src=\"https://trmnl.com/js/latest/plugins.js\"></script>
    <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">
    <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>
    <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;350;375;400;450;600;700&display=swap\" rel=\"stylesheet\">
    <title>{PREVIEW_DOCUMENT_TITLE}</title>
  </head>
  <body class=\"environment trmnl\">
    <div class=\"screen\">
      <div class=\"view view--full\">
{rendered_markup}
      </div>
    </div>
  </body>
</html>
"""


def render_preview_html(
    output_path: str,
    merge_variables: Dict[str, Any],
    *,
    program_name: str,
    markup_path: Optional[str] = None,
) -> None:
    """Render a standalone HTML preview directly from the plugin Liquid template."""
    try:
        from liquid import Environment as LiquidEnvironment
        from liquid import FileSystemLoader as LiquidFileSystemLoader
    except ImportError as exc:
        raise RuntimeError(
            "Preview rendering from .liquid requires the 'python-liquid' package. "
            "Install dependencies from requirements.txt."
        ) from exc

    resolved_markup_path = markup_path or _resolve_markup_path(program_name)
    env = LiquidEnvironment(loader=LiquidFileSystemLoader(os.path.dirname(resolved_markup_path)))
    template = env.get_template(os.path.basename(resolved_markup_path))
    rendered_markup = template.render(**merge_variables)
    html = _build_preview_document(rendered_markup)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


def parse_args(argv: Optional[List[str]] = None, *, program_name: Optional[str] = None) -> argparse.Namespace:
    """Parse command-line arguments for live mode, preview mode, and diagnostics."""
    resolved_program_name = program_name or _resolve_program_name(argv)
    primary_markup_name = _resolve_primary_markup_name(resolved_program_name)
    primary_markup_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), primary_markup_name)
    fallback_markup_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daypulse_markup.liquid")
    markup_name = primary_markup_name if os.path.exists(primary_markup_path) else os.path.basename(fallback_markup_path)
    epilog = f"""
Examples:
  # Default: fetch data and POST to TRMNL
    python {resolved_program_name}

  # Preview only (no send)
  python {resolved_program_name} --config config.yaml --no-send --preview-html preview.html

  # Log to file
  python {resolved_program_name} --config config.yaml --log-mode file --log-file trmnl.log
""".strip()

    p = argparse.ArgumentParser(
        prog=resolved_program_name,
        description="Generate a TRMNL dashboard payload (weather + markets + calendar) and send it via TRMNL webhook.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    p.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml).",
    )
    p.add_argument(
        "--translations",
        help="Path to translations YAML (overrides general.paths.translations_yaml).",
    )
    p.add_argument(
        "--markup",
        help="Path to Liquid markup template (overrides general.paths.markup_liquid).",
    )
    p.add_argument("--no-send", action="store_true", help="Do not POST to TRMNL.")
    p.add_argument(
        "--preview-html",
        help=f"Write a standalone preview HTML rendered directly from {markup_name}.",
    )
    p.add_argument(
        "--print-payload",
        action="store_true",
        help="Print the JSON webhook payload to stdout (merge_variables only).",
    )
    p.add_argument(
        "--list-calendars",
        action="store_true",
        help="List accessible Google Calendar calendar IDs and exit.",
    )
    p.add_argument(
        "--log-payload",
        action="store_true",
        help="Log the full JSON webhook payload at INFO level before preview/send.",
    )

    p.add_argument(
        "--log-mode",
        choices=["console", "file", "both", "none"],
        help="Override config general.log.mode.",
    )
    p.add_argument("--log-level", help="Override config general.log.level (e.g. CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET).")
    p.add_argument("--log-file", help="Override config general.log.file.")

    p.add_argument(
        "--test-random",
        action="store_true",
        help="Generate random demo data (simulates failures) instead of calling external APIs.",
    )
    p.add_argument(
        "--test-seed",
        type=int,
        help="Seed for --test-random to get reproducible output.",
    )
    p.add_argument(
        "--test-failure-rate",
        type=float,
        default=0.30,
        help="Chance (0..1) that each data source fails in --test-random mode (default: 0.30).",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """Load configuration, build payload data, optionally preview it, and send it to TRMNL."""
    program_name = _resolve_program_name(argv)
    args = parse_args(argv, program_name=program_name)

    started = dt.datetime.now(dt.timezone.utc)
    config_path = os.path.abspath(args.config)
    config_dir = os.path.dirname(config_path)
    config = _load_yaml(config_path)
    translations_path = _get_translations_path(config, args.translations, config_dir=config_dir)

    general = config.get("general", {})
    log_cfg = (general.get("log") or {}) if isinstance(general, dict) else {}
    log_mode = str(args.log_mode or log_cfg.get("mode", "file")).strip().lower()
    log_level = str(args.log_level or log_cfg.get("level", "WARNING")).strip().upper()
    log_file = str(args.log_file or log_cfg.get("file", "")).strip() or None
    log_max_bytes = _resolve_positive_int(log_cfg.get("max_bytes", 0), 0, minimum=0)
    log_backup_count = _resolve_positive_int(log_cfg.get("backup_count", 0), 0, minimum=0)
    _setup_logging(
        log_mode,
        log_level,
        log_file,
        max_bytes=log_max_bytes,
        backup_count=log_backup_count,
    )

    LOGGER.info("Script start")
    LOGGER.info("Configuration loaded: %s", config_path)
    LOGGER.info("Translations file: %s", translations_path)
    LOGGER.info(
        "Logging configured: mode=%s level=%s file=%s max_bytes=%s backup_count=%s",
        log_mode,
        log_level,
        log_file or "-",
        log_max_bytes,
        log_backup_count,
    )
    LOGGER.debug("Args: %s", vars(args))

    if args.list_calendars:
        calendars = list_google_calendars(config)
        LOGGER.info("Google Calendar listing OK - %s calendar(s) found", len(calendars))
        print(json.dumps(calendars, indent=2, ensure_ascii=False))
        return 0

    translations = _load_yaml(translations_path)

    trmnl_cfg = config.get("trmnl", {})
    webhook_url = str(trmnl_cfg.get("webhook_url", "")).strip()
    timeout_seconds = int(trmnl_cfg.get("timeout_seconds", 15))
    payload_soft_limit_bytes = _get_trmnl_payload_soft_limit_bytes(config)
    if not webhook_url:
        raise ValueError("trmnl.webhook_url is required")

    finance_entries = _get_finance_entries(config)
    weather_cfg = _as_dict(config.get("weather"))
    weather_location_summary = str(weather_cfg.get("address", "")).strip()
    if not weather_location_summary:
        legacy_city = str(weather_cfg.get("city", "")).strip()
        if legacy_city:
            weather_location_summary = legacy_city
        elif weather_cfg.get("latitude") is not None and weather_cfg.get("longitude") is not None:
            weather_location_summary = f"{weather_cfg.get('latitude')},{weather_cfg.get('longitude')}"
    LOGGER.info(
        "Config summary: weather_location=%s tickers=%s label_overrides=%s calendar_mode=%s timeout=%ss payload_soft_limit=%s",
        weather_location_summary or "-",
        len(finance_entries),
        len([entry for entry in finance_entries if entry.get("label")]),
        str(_as_dict(config.get("google_calendar")).get("mode", "oauth")).strip().lower(),
        timeout_seconds,
        payload_soft_limit_bytes,
    )

    if args.test_random:
        merge_variables = build_merge_variables_random(
            config,
            translations,
            seed=args.test_seed,
            failure_rate=args.test_failure_rate,
        )
    else:
        merge_variables = build_merge_variables(config, translations)

    # High-level summary logs
    try:
        LOGGER.info(
            "Data summary: weather_ok=%s finance_ok=%s calendar_ok=%s indices=%s cal_days=%s",
            bool(merge_variables.get("weather", {}).get("ok")),
            bool(merge_variables.get("finance", {}).get("ok")),
            bool(merge_variables.get("calendar", {}).get("ok")),
            len(merge_variables.get("finance", {}).get("indices") or []),
            len(merge_variables.get("calendar", {}).get("days") or []),
        )
        LOGGER.debug("Meta: %s", merge_variables.get("meta"))
    except Exception:
        pass

    compact_merge_variables = _compact_merge_variables(merge_variables)
    payload_json = _build_trmnl_payload_json(compact_merge_variables)
    _log_payload_size(payload_json, soft_limit_bytes=payload_soft_limit_bytes)

    if args.log_payload:
        LOGGER.info(
            "Full payload JSON:\n%s",
            json.dumps({"merge_variables": compact_merge_variables}, ensure_ascii=False, indent=2),
        )

    if LOGGER.isEnabledFor(logging.DEBUG):
        # Avoid spamming huge logs; keep a short preview of the payload.
        payload_preview = json.dumps(
            {"merge_variables": compact_merge_variables},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        LOGGER.debug("Payload JSON preview (first 2000 chars): %s", payload_preview[:2000])

    if args.print_payload:
        print(json.dumps({"merge_variables": compact_merge_variables}, ensure_ascii=False, indent=2))

    if args.preview_html:
        markup_path = _get_markup_path(config, args.markup, config_dir=config_dir, program_name=program_name)
        LOGGER.info("Liquid markup file: %s", markup_path)
        render_preview_html(
            args.preview_html,
            compact_merge_variables,
            program_name=program_name,
            markup_path=markup_path,
        )
        LOGGER.info("Wrote preview HTML: %s", args.preview_html)

    exit_code = 0
    if args.no_send:
        LOGGER.info("--no-send set; skipping TRMNL POST")
    else:
        resp = send_to_trmnl(config, webhook_url, compact_merge_variables, timeout_seconds)
        LOGGER.info("TRMNL response: HTTP %s - %s", resp.status_code, _describe_trmnl_status(resp.status_code))
        if resp.status_code == 429:
            LOGGER.error("TRMNL rate limit hit (429). Consider sending less often.")
        if resp.status_code >= 400:
            LOGGER.error("TRMNL webhook failed (%s): %s", resp.status_code, resp.text[:500])
            exit_code = 2
        else:
            LOGGER.info("TRMNL webhook OK (%s)", resp.status_code)

    elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
    LOGGER.info("Script end (elapsed=%.3fs)", elapsed)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
