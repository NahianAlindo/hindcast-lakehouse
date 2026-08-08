"""Pydantic response-validation models per OpenWeatherMap endpoint.

Bronze is lossless by contract (docs/PLAN.md): a validation failure here is
logged, never a reason to drop the payload. Callers try to parse the model,
and land the raw payload regardless of whether parsing succeeded.
"""

from pydantic import BaseModel


class Coord(BaseModel):
    lon: float
    lat: float


class WeatherCondition(BaseModel):
    id: int
    main: str
    description: str
    icon: str


class MainWeather(BaseModel):
    temp: float
    feels_like: float
    temp_min: float
    temp_max: float
    pressure: int
    humidity: int


class Wind(BaseModel):
    speed: float
    deg: int | None = None
    gust: float | None = None


class CurrentWeatherResponse(BaseModel):
    coord: Coord
    weather: list[WeatherCondition]
    main: MainWeather
    wind: Wind | None = None
    dt: int
    name: str
    cod: int


class ForecastTimestep(BaseModel):
    dt: int
    main: MainWeather
    weather: list[WeatherCondition]
    dt_txt: str
    pop: float | None = None


class ForecastCity(BaseModel):
    id: int
    name: str
    coord: Coord


class ForecastResponse(BaseModel):
    cod: str
    cnt: int
    list: list[ForecastTimestep]
    city: ForecastCity


class AirQualityComponents(BaseModel):
    co: float
    no: float | None = None
    no2: float
    o3: float
    so2: float
    pm2_5: float
    pm10: float
    nh3: float


class AirQualityItem(BaseModel):
    main: dict
    components: AirQualityComponents
    dt: int


class AirPollutionResponse(BaseModel):
    coord: Coord
    list: list[AirQualityItem]


def validate(endpoint: str, payload: dict) -> tuple[bool, str | None]:
    """Returns (is_valid, error_message). Never raises."""
    model_by_endpoint = {
        "current": CurrentWeatherResponse,
        "forecast": ForecastResponse,
        "air_quality": AirPollutionResponse,
    }
    model = model_by_endpoint.get(endpoint)
    if model is None:
        return False, f"no validation model registered for endpoint '{endpoint}'"
    try:
        model.model_validate(payload)
        return True, None
    except Exception as exc:  # noqa: BLE001  # deliberately broad, see docstring
        return False, str(exc)
