"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
from pathlib import Path
import csv
import json
import math
import pendulum
import random
import requests
import statistics

# Coordinates of NASA Johnson Space Center, Houston, TX
JSC_LATITUDE = 29.5593
JSC_LONGITUDE = -95.0907

# Nominal long-duration crew capacity per spacecraft, used for capacity
# metrics. Crafts not listed here are reported without a capacity.
SPACECRAFT_CAPACITY = {
    "ISS": 7,
    "Tiangong": 3,
}

# Data quality thresholds used by `validate_data_quality`
# Maximum age of the ISS position and the weather observation for the data
# to count as timely.
ISS_MAX_AGE_MINUTES = 10
WEATHER_MAX_AGE_MINUTES = 120
# The ISS orbit is inclined ~51.6 degrees, so it never goes beyond this latitude.
ISS_MAX_ABS_LATITUDE = 52.0
# Plausible ranges for weather readings in Houston, TX: (min, max)
WEATHER_RANGES = {
    "temperature_c": (-20, 50),
    "apparent_temperature_c": (-30, 60),
    "relative_humidity_pct": (0, 100),
    "precipitation_mm": (0, 200),
    "cloud_cover_pct": (0, 100),
    "surface_pressure_hpa": (870, 1085),
    "wind_speed_kmh": (0, 300),
    "wind_direction_deg": (0, 360),
}

# Settings used by the analytics tasks (`perform_statistical_methods`,
# `calculate_trends`, `perform_comprehensive_analysis`, `generate_insights_report`)
PERCENTILES = (5, 10, 25, 50, 75, 90, 95)
# Samples here are small (a handful of astronauts/crafts), so a z-score of 2
# is used rather than the textbook 3 to flag outliers.
Z_SCORE_OUTLIER_THRESHOLD = 2.0
# Minimum sample size before running the Jarque-Bera normality test.
MIN_NORMALITY_SAMPLE = 8
# Number of past runs kept as history for trend analysis.
TREND_HISTORY_LENGTH = 30
MOVING_AVERAGE_WINDOWS = (3, 7)
# A trend whose slope is under 1% of the series mean per run counts as stable.
TREND_STABILITY_THRESHOLD = 0.01
# Days to act on a recommendation, by priority.
DECISION_WINDOW_DAYS = {"high": 1, "medium": 7, "low": 30}

# Color-coded thresholds used by `generate_performance_dashboard`
# Capacity utilization (%): GREEN below the first, AMBER up to the second,
# RED above it.
UTILIZATION_THRESHOLDS = (85, 100)
# Data quality pass rate (%): GREEN at or above the first, AMBER at or above
# the second, RED below it.
DATA_QUALITY_THRESHOLDS = (100, 90)
# Weather at JSC: AMBER at or above this wind speed (km/h), RED at the second.
WIND_THRESHOLDS_KMH = (40, 60)
STATUS_RANK = {"GREEN": 0, "AMBER": 1, "RED": 2, "GREY": -1}

# Settings used by `build_regression_model`
# Number of historical periods the model trains on. Periods not covered by a
# real run in the `calculate_trends` history are filled with synthetic data.
REGRESSION_PERIODS = 30
# Number of future periods to predict.
PREDICTION_HORIZON_PERIODS = 5
# Std dev of the noise on synthetic astronaut counts, used when the trend
# history has no volatility to go on yet.
SYNTHETIC_NOISE_SD = 0.6
# Std dev of synthetic weather around the observed reading.
SYNTHETIC_WEATHER_SD = {
    "temperature_c": 3.0,
    "relative_humidity_pct": 8.0,
    "surface_pressure_hpa": 4.0,
}
# Share of the latest change (momentum) carried into each future period;
# it decays geometrically with each period ahead.
MOMENTUM_DECAY = 0.5
# Raise a population growth/decline alert when the last prediction differs
# from the current count by at least this percentage.
POPULATION_ALERT_PCT = 10
# Model fit quality bands on R squared, checked from the top.
FIT_QUALITY_BANDS = (
    (0.9, "Excellent"),
    (0.7, "Good"),
    (0.5, "Moderate"),
    (0.0, "Poor"),
)
# Two-sided 95% critical values of Student's t, by degrees of freedom.
T_CRITICAL_95 = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    12: 2.179,
    15: 2.131,
    20: 2.086,
    25: 2.060,
    30: 2.042,
    40: 2.021,
    60: 2.000,
    120: 1.980,
}


def _percentile(ordered: list[float], pct: float) -> float:
    """Percentile of a sorted list, using linear interpolation."""
    k = (len(ordered) - 1) * pct / 100
    lower = math.floor(k)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (k - lower)


def _moments(values: list[float]) -> dict:
    """Mean, population std dev, skewness and excess kurtosis."""
    n = len(values)
    mean = sum(values) / n
    m2 = sum((v - mean) ** 2 for v in values) / n
    m3 = sum((v - mean) ** 3 for v in values) / n
    m4 = sum((v - mean) ** 4 for v in values) / n
    if m2 == 0:
        return {"mean": mean, "std_dev": 0.0, "skewness": None, "kurtosis": None}
    return {
        "mean": mean,
        "std_dev": math.sqrt(m2),
        "skewness": m3 / m2**1.5,
        "kurtosis": m4 / m2**2 - 3,
    }


def _linear_trend(values: list[float]) -> dict:
    """Least-squares line through the values, with x = 0, 1, 2, ..."""
    n = len(values)
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    sxx = sum((x - x_mean) ** 2 for x in range(n))
    sxy = sum((x - x_mean) * (y - y_mean) for x, y in enumerate(values))
    slope = sxy / sxx
    intercept = y_mean - slope * x_mean
    ss_tot = sum((y - y_mean) ** 2 for y in values)
    ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in enumerate(values))
    r_squared = 1 - ss_res / ss_tot if ss_tot else 1.0
    return {"slope": slope, "intercept": intercept, "r_squared": r_squared}


def _least_squares(xs: list[float], ys: list[float]) -> dict | None:
    """Simple linear regression y = intercept + slope * x by least squares.

    Returns None when x has no variance (the line is undefined).
    """
    n = len(xs)
    x_mean, y_mean = sum(xs) / n, sum(ys) / n
    sxx = sum((x - x_mean) ** 2 for x in xs)
    if sxx == 0:
        return None
    sxy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=False))
    syy = sum((y - y_mean) ** 2 for y in ys)
    slope = sxy / sxx
    return {
        "slope": slope,
        "intercept": y_mean - slope * x_mean,
        "pearson_r": sxy / math.sqrt(sxx * syy) if syy else 0.0,
    }


def _t_critical_95(dof: int) -> float:
    """Two-sided 95% t value; uses the nearest tabulated dof at or below."""
    if dof < 1:
        return T_CRITICAL_95[1]
    eligible = [df for df in T_CRITICAL_95 if df <= dof]
    return T_CRITICAL_95[max(eligible)] if dof <= 120 else 1.96


# Settings used by `compare_regression_models` and `cross_validate_models`
# (name, polynomial degree) of each model compared.
COMPARISON_MODELS = (
    ("Linear", 1),
    ("Polynomial (degree 2)", 2),
    ("Polynomial (degree 3)", 3),
)
# Weights of the composite score used to rank models (must sum to 1).
COMPOSITE_WEIGHTS = {"adjusted_r_squared": 0.4, "rmse": 0.3, "aic": 0.3}
MEDALS = ("Gold", "Silver", "Bronze")
# Number of folds for k-fold cross-validation.
CV_FOLDS = 5
# Smallest training window for walk-forward backtesting.
BACKTEST_MIN_TRAIN = 10

# Where `export_analysis_report` writes its files. On Astro, /tmp is local to
# the worker that runs the task and is not kept after the task ends; point
# this at shared or object storage for real downstream consumption.
EXPORT_BASE_DIR = "/tmp/astronaut_reports"


def _solve_linear_system(
    matrix: list[list[float]], vector: list[float]
) -> list[float] | None:
    """Solve A x = b by Gaussian elimination with partial pivoting.

    Returns None when A is singular.
    """
    size = len(vector)
    aug = [row[:] + [vector[i]] for i, row in enumerate(matrix)]
    for col in range(size):
        pivot = max(range(col, size), key=lambda r: abs(aug[r][col]))
        if abs(aug[pivot][col]) < 1e-12:
            return None
        aug[col], aug[pivot] = aug[pivot], aug[col]
        for r in range(col + 1, size):
            factor = aug[r][col] / aug[col][col]
            for c in range(col, size + 1):
                aug[r][c] -= factor * aug[col][c]
    solution = [0.0] * size
    for r in range(size - 1, -1, -1):
        solution[r] = (
            aug[r][size] - sum(aug[r][c] * solution[c] for c in range(r + 1, size))
        ) / aug[r][r]
    return solution


def _fit_polynomial(xs: list[float], ys: list[float], degree: int) -> dict | None:
    """Least-squares polynomial fit using the normal equations (X'X)b = X'y.

    x is centered and scaled before fitting so the normal equations stay well
    conditioned for degree 3. Returns None when the system is singular.
    """
    n = len(xs)
    center = sum(xs) / n
    scale = math.sqrt(sum((x - center) ** 2 for x in xs) / n) or 1.0
    rows = [[((x - center) / scale) ** p for p in range(degree + 1)] for x in xs]
    xtx = [
        [sum(row[a] * row[b] for row in rows) for b in range(degree + 1)]
        for a in range(degree + 1)
    ]
    xty = [
        sum(row[a] * y for row, y in zip(rows, ys, strict=False))
        for a in range(degree + 1)
    ]
    coefficients = _solve_linear_system(xtx, xty)
    if coefficients is None:
        return None
    return {
        "degree": degree,
        "center": center,
        "scale": scale,
        "coefficients": coefficients,
    }


def _predict_polynomial(model: dict, x: float) -> float:
    z = (x - model["center"]) / model["scale"]
    return sum(c * z**p for p, c in enumerate(model["coefficients"]))


def _polynomial_equation(model: dict) -> str:
    """Equation in terms of the raw period t, expanding the scaled fit."""
    m, s, c = model["center"], model["scale"], model["coefficients"]
    degree = len(c) - 1
    raw = [
        sum(
            c[k] * math.comb(k, j) * (-m) ** (k - j) / s**k
            for k in range(j, degree + 1)
        )
        for j in range(degree + 1)
    ]
    terms = [f"{raw[0]:.4f}"]
    for power in range(1, degree + 1):
        sign = "+" if raw[power] >= 0 else "-"
        suffix = "t" if power == 1 else f"t^{power}"
        terms.append(f"{sign} {abs(raw[power]):.4g}*{suffix}")
    return "y = " + " ".join(terms)


def _error_metrics(actual: list[float], predicted: list[float]) -> dict:
    """Forecast error metrics: RMSE, MAE, MAPE, bias, max error, R squared."""
    n = len(actual)
    if n == 0:
        return {"n": 0}
    errors = [p - a for a, p in zip(actual, predicted, strict=False)]
    sse = sum(e**2 for e in errors)
    mean = sum(actual) / n
    sst = sum((a - mean) ** 2 for a in actual)
    positive = [(a, e) for a, e in zip(actual, errors, strict=False) if a > 0]
    return {
        "n": n,
        "rmse": round(math.sqrt(sse / n), 4),
        "mae": round(sum(abs(e) for e in errors) / n, 4),
        "mape_pct": round(sum(abs(e) / a for a, e in positive) / len(positive) * 100, 3)
        if positive
        else None,
        "bias": round(sum(errors) / n, 4),
        "max_abs_error": round(max(abs(e) for e in errors), 4),
        "r_squared": round(1 - sse / sst, 4) if sst else None,
    }


# Datasets shared between the producer DAG and the consumer DAG in this file
current_astronauts_dataset = Dataset("current_astronauts")
iss_location_dataset = Dataset("iss_location")
# A third dataset example: updated by `summarise_spacecraft`, its events carry
# extra metadata (a per-spacecraft headcount) that downstream DAGs can read.
spacecraft_summary_dataset = Dataset("spacecraft_summary")
# Updated by `build_regression_model` with its predictions.
regression_predictions_dataset = Dataset("regression_predictions")
# Updated by `compare_regression_models` with the model ranking.
model_comparison_dataset = Dataset("model_comparison")


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():
    # Define tasks
    @task(
        # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[current_astronauts_dataset]
    )  # Define that this task updates the `current_astronauts` Dataset
    def get_astronauts(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        r = requests.get("http://api.open-notify.org/astros.json")
        number_of_people_in_space = r.json()["number"]
        list_of_people_in_space = r.json()["people"]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    @task(
        # A second dataset example: this task updates the `iss_location` Dataset
        outlets=[iss_location_dataset]
    )
    def get_iss_location() -> dict:
        """
        This task retrieves the current position of the International Space
        Station from the Open Notify API. Because it declares the
        `iss_location` Dataset as an outlet, every successful run of this task
        marks the Dataset as updated, which can trigger downstream DAGs.
        """
        r = requests.get("http://api.open-notify.org/iss-now.json", timeout=30)
        r.raise_for_status()
        position = r.json()["iss_position"]
        location = {
            "latitude": float(position["latitude"]),
            "longitude": float(position["longitude"]),
            "timestamp": r.json()["timestamp"],
        }
        print(
            f"The ISS is currently at latitude {location['latitude']}, "
            f"longitude {location['longitude']}."
        )
        return location

    @task(
        # Define that this task updates the `spacecraft_summary` Dataset, so
        # downstream DAGs scheduled on it run after each aggregation.
        outlets=[spacecraft_summary_dataset]
    )
    def summarise_spacecraft(people_in_space: list[dict], **context) -> dict:
        """
        This task aggregates the astronaut data by spacecraft, counting the
        number of astronauts on each spacecraft. The output is saved as the
        `spacecraft_summary` Dataset, which can trigger downstream tasks in
        other DAGs. The counts are also attached to the Dataset event as
        `extra` metadata and returned via XCom.
        """
        spacecraft_counts: dict[str, int] = {}
        for person in people_in_space:
            craft = person.get("craft", "Unknown")
            spacecraft_counts[craft] = spacecraft_counts.get(craft, 0) + 1

        summary = {
            "spacecraft_counts": spacecraft_counts,
            "total_astronauts": len(people_in_space),
            "spacecraft_count": len(spacecraft_counts),
        }

        context["outlet_events"][spacecraft_summary_dataset].extra = summary

        for craft, count in sorted(
            spacecraft_counts.items(), key=lambda item: item[1], reverse=True
        ):
            print(f"  - {craft}: {count} astronaut(s)")
        return summary

    @task
    def get_weather_data() -> dict:
        """
        This task fetches weather data from the Open-Meteo API (free, no API
        key required). It uses the coordinates of Houston, TX (NASA Johnson
        Space Center) to get relevant weather data, and returns weather
        metrics that will be correlated with astronaut data.
        """
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": JSC_LATITUDE,
                "longitude": JSC_LONGITUDE,
                "current": ",".join(
                    [
                        "temperature_2m",
                        "relative_humidity_2m",
                        "apparent_temperature",
                        "precipitation",
                        "cloud_cover",
                        "surface_pressure",
                        "wind_speed_10m",
                        "wind_direction_10m",
                        "weather_code",
                    ]
                ),
                "timezone": "America/Chicago",
            },
            timeout=30,
        )
        r.raise_for_status()
        current = r.json()["current"]

        weather = {
            "location": "Houston, TX (NASA Johnson Space Center)",
            "latitude": JSC_LATITUDE,
            "longitude": JSC_LONGITUDE,
            "observed_at": current.get("time"),
            "temperature_c": current.get("temperature_2m"),
            "apparent_temperature_c": current.get("apparent_temperature"),
            "relative_humidity_pct": current.get("relative_humidity_2m"),
            "precipitation_mm": current.get("precipitation"),
            "cloud_cover_pct": current.get("cloud_cover"),
            "surface_pressure_hpa": current.get("surface_pressure"),
            "wind_speed_kmh": current.get("wind_speed_10m"),
            "wind_direction_deg": current.get("wind_direction_10m"),
            "weather_code": current.get("weather_code"),
        }
        print(
            f"Weather at {weather['location']} ({weather['observed_at']}): "
            f"{weather['temperature_c']}°C, "
            f"{weather['relative_humidity_pct']}% humidity, "
            f"wind {weather['wind_speed_kmh']} km/h, "
            f"cloud cover {weather['cloud_cover_pct']}%"
        )
        return weather

    @task
    def calculate_astronauts_stats(people_in_space: list[dict]) -> dict:
        """
        This task performs advanced statistical calculations on astronaut
        data, including name length analysis, spacecraft capacity metrics,
        distribution patterns, and statistical measures (mean, median,
        variance, etc.).
        """

        def describe(values: list[float]) -> dict:
            """Mean, median, variance, standard deviation, min, max and range."""
            if not values:
                return {"count": 0}
            return {
                "count": len(values),
                "mean": round(statistics.mean(values), 2),
                "median": statistics.median(values),
                "mode": statistics.mode(values),
                "variance": round(statistics.pvariance(values), 2),
                "std_dev": round(statistics.pstdev(values), 2),
                "min": min(values),
                "max": max(values),
                "range": max(values) - min(values),
            }

        # Name length analysis
        names = [p.get("name", "").strip() for p in people_in_space]
        names = [name for name in names if name]
        name_lengths = [len(name) for name in names]
        name_length_stats = describe(name_lengths)
        if names:
            name_length_stats["longest_name"] = max(names, key=len)
            name_length_stats["shortest_name"] = min(names, key=len)

        # Distribution patterns: astronauts per spacecraft
        crew_counts: dict[str, int] = {}
        for person in people_in_space:
            craft = person.get("craft", "Unknown")
            crew_counts[craft] = crew_counts.get(craft, 0) + 1
        total = len(people_in_space)
        distribution = {
            craft: {
                "count": count,
                "share_pct": round(count / total * 100, 2) if total else 0.0,
            }
            for craft, count in sorted(
                crew_counts.items(), key=lambda item: item[1], reverse=True
            )
        }
        crew_size_stats = describe(list(crew_counts.values()))

        # Spacecraft capacity metrics, against nominal long-duration crew sizes
        capacity_metrics = {}
        for craft, count in crew_counts.items():
            capacity = SPACECRAFT_CAPACITY.get(craft)
            capacity_metrics[craft] = {
                "crew": count,
                "nominal_capacity": capacity,
                "utilization_pct": round(count / capacity * 100, 2)
                if capacity
                else None,
                "over_nominal_capacity": count > capacity if capacity else None,
            }

        stats = {
            "total_astronauts": total,
            "name_length_stats": name_length_stats,
            "crew_size_stats": crew_size_stats,
            "distribution": distribution,
            "capacity_metrics": capacity_metrics,
        }

        print(f"Total astronauts: {total}")
        print(f"Name length stats: {name_length_stats}")
        print(f"Crew size per spacecraft stats: {crew_size_stats}")
        for craft, metrics in capacity_metrics.items():
            print(f"  - {craft}: {metrics}")
        return stats

    @task
    def analyze_correlation(weather: dict, astronaut_stats: dict) -> dict:
        """
        This task combines the Houston weather metrics with the astronaut
        statistics into a single snapshot. It pairs each weather metric with
        the astronaut metrics for this run and derives simple indicators,
        such as whether conditions at Johnson Space Center are calm and clear.

        A single run gives one data point per metric, so this is a snapshot
        and not a statistical correlation. Collecting these snapshots across
        runs gives the series needed for one.
        """
        total_astronauts = astronaut_stats.get("total_astronauts", 0)
        name_stats = astronaut_stats.get("name_length_stats", {})
        crew_stats = astronaut_stats.get("crew_size_stats", {})

        temperature = weather.get("temperature_c")
        wind_speed = weather.get("wind_speed_kmh")
        cloud_cover = weather.get("cloud_cover_pct")
        precipitation = weather.get("precipitation_mm")

        # Calm and clear: light wind, no precipitation, mostly clear skies
        calm_and_clear = (
            wind_speed is not None
            and cloud_cover is not None
            and precipitation is not None
            and wind_speed < 20
            and precipitation == 0
            and cloud_cover < 50
        )

        astronauts_per_degree = (
            round(total_astronauts / temperature, 4) if temperature else None
        )

        analysis = {
            "observed_at": weather.get("observed_at"),
            "location": weather.get("location"),
            "weather": {
                "temperature_c": temperature,
                "relative_humidity_pct": weather.get("relative_humidity_pct"),
                "wind_speed_kmh": wind_speed,
                "cloud_cover_pct": cloud_cover,
                "precipitation_mm": precipitation,
                "surface_pressure_hpa": weather.get("surface_pressure_hpa"),
            },
            "astronauts": {
                "total_astronauts": total_astronauts,
                "mean_name_length": name_stats.get("mean"),
                "mean_crew_size": crew_stats.get("mean"),
                "spacecraft_count": crew_stats.get("count", 0),
            },
            "indicators": {
                "calm_and_clear_at_jsc": calm_and_clear,
                "astronauts_per_degree_c": astronauts_per_degree,
            },
        }

        print(
            f"{total_astronauts} astronauts in space while Houston reports "
            f"{temperature}°C, wind {wind_speed} km/h, "
            f"cloud cover {cloud_cover}%."
        )
        print(f"Calm and clear at JSC: {calm_and_clear}")
        print(f"Correlation snapshot: {analysis}")
        return analysis

    @task
    def generate_summary_report(
        people_in_space: list[dict],
        weather: dict,
        correlation: dict,
        astronaut_stats: dict,
        **context,
    ) -> dict:
        """
        This task generates a comprehensive summary report by consolidating
        data from all major pipeline tasks: astronaut data, weather data,
        correlation analysis, and calculated statistics. It provides an
        executive summary with key findings, metrics, and actionable insights.
        """
        total_astronauts = astronaut_stats.get("total_astronauts", 0)
        distribution = astronaut_stats.get("distribution", {})
        capacity_metrics = astronaut_stats.get("capacity_metrics", {})
        name_stats = astronaut_stats.get("name_length_stats", {})
        crew_stats = astronaut_stats.get("crew_size_stats", {})
        indicators = correlation.get("indicators", {})

        # Key findings
        key_findings = [
            f"{total_astronauts} astronaut(s) are in space across "
            f"{len(distribution)} spacecraft."
        ]
        if distribution:
            busiest_craft, busiest = next(iter(distribution.items()))
            key_findings.append(
                f"{busiest_craft} carries the most crew: {busiest['count']} "
                f"({busiest['share_pct']}% of all astronauts)."
            )
        if name_stats.get("count"):
            key_findings.append(
                f"Astronaut names average {name_stats['mean']} characters; "
                f"longest is {name_stats.get('longest_name')}."
            )
        key_findings.append(
            f"Houston (JSC) reports {weather.get('temperature_c')}°C, "
            f"wind {weather.get('wind_speed_kmh')} km/h, cloud cover "
            f"{weather.get('cloud_cover_pct')}%; calm and clear: "
            f"{indicators.get('calm_and_clear_at_jsc')}."
        )

        # Actionable insights
        insights = []
        for craft, metrics in capacity_metrics.items():
            if metrics.get("over_nominal_capacity"):
                insights.append(
                    f"{craft} is above nominal capacity "
                    f"({metrics['crew']}/{metrics['nominal_capacity']}, "
                    f"{metrics['utilization_pct']}%). Review crew rotation."
                )
            elif metrics.get("nominal_capacity") is None:
                insights.append(
                    f"{craft} has no nominal capacity defined. Add it to "
                    f"SPACECRAFT_CAPACITY to track utilization."
                )
        if len(people_in_space) != total_astronauts:
            insights.append(
                f"Data mismatch: {len(people_in_space)} astronauts from the "
                f"API vs {total_astronauts} in statistics. Check the pipeline."
            )
        missing_weather = [
            key
            for key in ("temperature_c", "wind_speed_kmh", "cloud_cover_pct")
            if weather.get(key) is None
        ]
        if missing_weather:
            insights.append(
                f"Weather data is incomplete (missing: "
                f"{', '.join(missing_weather)}). Check the Open-Meteo API."
            )
        if indicators.get("calm_and_clear_at_jsc"):
            insights.append(
                "Conditions at JSC are calm and clear, suitable for "
                "outdoor operations and ground observations."
            )
        if not insights:
            insights.append("No issues detected. All metrics are nominal.")

        report = {
            "report_date": context["ds"],
            "run_id": context["run_id"],
            "executive_summary": {
                "total_astronauts": total_astronauts,
                "spacecraft_count": len(distribution),
                "weather_location": weather.get("location"),
                "weather_observed_at": weather.get("observed_at"),
                "calm_and_clear_at_jsc": indicators.get("calm_and_clear_at_jsc"),
            },
            "key_metrics": {
                "crew_distribution": distribution,
                "capacity_metrics": capacity_metrics,
                "mean_crew_size": crew_stats.get("mean"),
                "mean_name_length": name_stats.get("mean"),
                "temperature_c": weather.get("temperature_c"),
                "relative_humidity_pct": weather.get("relative_humidity_pct"),
                "wind_speed_kmh": weather.get("wind_speed_kmh"),
                "cloud_cover_pct": weather.get("cloud_cover_pct"),
                "astronauts_per_degree_c": indicators.get("astronauts_per_degree_c"),
            },
            "key_findings": key_findings,
            "actionable_insights": insights,
        }

        print("=" * 60)
        print(f"ASTRONAUT PIPELINE SUMMARY REPORT - {report['report_date']}")
        print("=" * 60)
        print("Key findings:")
        for finding in key_findings:
            print(f"  - {finding}")
        print("Actionable insights:")
        for insight in insights:
            print(f"  - {insight}")
        print("=" * 60)
        return report

    @task
    def validate_data_quality(
        people_in_space: list[dict],
        weather: dict,
        iss_location: dict,
        spacecraft_summary: dict,
        astronaut_stats: dict,
        correlation: dict,
        **context,
    ) -> dict:
        """
        This task performs comprehensive data quality validation across all
        data sources in the pipeline (astronauts, ISS location, weather, and
        the derived summary, statistics and correlation). It checks for
        completeness, accuracy, consistency, and timeliness of the data, and
        returns a detailed validation report with a pass/fail status for each
        check. The report is logged and pushed to XCom; failed checks do not
        fail the task.
        """
        checks: list[dict] = []

        def add_check(
            dimension: str, source: str, name: str, passed: bool, details: str
        ) -> None:
            checks.append(
                {
                    "dimension": dimension,
                    "source": source,
                    "check": name,
                    "status": "PASS" if passed else "FAIL",
                    "details": details,
                }
            )

        def in_range(value, low, high) -> bool:
            return isinstance(value, (int, float)) and low <= value <= high

        now = pendulum.now("UTC")
        total = len(people_in_space)

        # ---------------- Completeness ----------------
        add_check(
            "completeness",
            "astronauts",
            "astronaut_list_not_empty",
            total > 0,
            f"{total} astronaut record(s) received.",
        )
        incomplete_people = [
            i
            for i, p in enumerate(people_in_space)
            if not str(p.get("name") or "").strip()
            or not str(p.get("craft") or "").strip()
        ]
        add_check(
            "completeness",
            "astronauts",
            "astronaut_required_fields",
            not incomplete_people,
            "All records have a name and craft."
            if not incomplete_people
            else f"Records missing name or craft at index: {incomplete_people}",
        )
        missing_weather = [
            key
            for key in ["observed_at", *WEATHER_RANGES, "weather_code"]
            if weather.get(key) is None
        ]
        add_check(
            "completeness",
            "weather",
            "weather_required_fields",
            not missing_weather,
            "All weather fields present."
            if not missing_weather
            else f"Missing weather fields: {missing_weather}",
        )
        missing_iss = [
            key
            for key in ("latitude", "longitude", "timestamp")
            if iss_location.get(key) is None
        ]
        add_check(
            "completeness",
            "iss_location",
            "iss_required_fields",
            not missing_iss,
            "All ISS location fields present."
            if not missing_iss
            else f"Missing ISS location fields: {missing_iss}",
        )

        # ---------------- Accuracy ----------------
        lat, lon = iss_location.get("latitude"), iss_location.get("longitude")
        add_check(
            "accuracy",
            "iss_location",
            "iss_coordinates_valid",
            in_range(lat, -90, 90) and in_range(lon, -180, 180),
            f"latitude={lat}, longitude={lon}",
        )
        add_check(
            "accuracy",
            "iss_location",
            "iss_latitude_within_orbit_inclination",
            in_range(lat, -ISS_MAX_ABS_LATITUDE, ISS_MAX_ABS_LATITUDE),
            f"|latitude| must be <= {ISS_MAX_ABS_LATITUDE}; got {lat}",
        )
        out_of_range = {
            key: weather.get(key)
            for key, (low, high) in WEATHER_RANGES.items()
            if weather.get(key) is not None
            and not in_range(weather.get(key), low, high)
        }
        add_check(
            "accuracy",
            "weather",
            "weather_values_in_plausible_range",
            not out_of_range,
            "All weather values within plausible ranges."
            if not out_of_range
            else f"Out-of-range values: {out_of_range}",
        )
        unknown_crafts = sorted(
            {p.get("craft") for p in people_in_space} - set(SPACECRAFT_CAPACITY)
        )
        add_check(
            "accuracy",
            "astronauts",
            "crafts_are_known",
            not unknown_crafts,
            "All crafts are in SPACECRAFT_CAPACITY."
            if not unknown_crafts
            else f"Unknown crafts: {unknown_crafts}",
        )

        # ---------------- Consistency ----------------
        reported_number = context["ti"].xcom_pull(
            task_ids="get_astronauts", key="number_of_people_in_space"
        )
        add_check(
            "consistency",
            "astronauts",
            "api_count_matches_records",
            reported_number == total,
            f"API reported {reported_number}, received {total} record(s).",
        )
        names = [str(p.get("name") or "").strip() for p in people_in_space]
        duplicates = sorted({n for n in names if n and names.count(n) > 1})
        add_check(
            "consistency",
            "astronauts",
            "no_duplicate_astronauts",
            not duplicates,
            "No duplicate names."
            if not duplicates
            else f"Duplicate names: {duplicates}",
        )
        summary_counts = spacecraft_summary.get("spacecraft_counts", {})
        add_check(
            "consistency",
            "spacecraft_summary",
            "summary_total_matches_source",
            spacecraft_summary.get("total_astronauts") == total
            and sum(summary_counts.values()) == total,
            f"summary total={spacecraft_summary.get('total_astronauts')}, "
            f"sum of craft counts={sum(summary_counts.values())}, "
            f"source={total}",
        )
        stats_counts = {
            craft: d.get("count")
            for craft, d in astronaut_stats.get("distribution", {}).items()
        }
        add_check(
            "consistency",
            "astronaut_stats",
            "stats_match_summary",
            astronaut_stats.get("total_astronauts") == total
            and stats_counts == summary_counts,
            f"stats total={astronaut_stats.get('total_astronauts')}, "
            f"stats per craft={stats_counts}, summary per craft={summary_counts}",
        )
        corr_total = correlation.get("astronauts", {}).get("total_astronauts")
        add_check(
            "consistency",
            "correlation",
            "correlation_matches_sources",
            corr_total == total
            and correlation.get("observed_at") == weather.get("observed_at"),
            f"correlation total={corr_total} (source {total}), "
            f"observed_at={correlation.get('observed_at')} "
            f"(weather {weather.get('observed_at')})",
        )

        # ---------------- Timeliness ----------------
        iss_ts = iss_location.get("timestamp")
        if isinstance(iss_ts, (int, float)):
            iss_age = (now - pendulum.from_timestamp(iss_ts, tz="UTC")).in_minutes()
            add_check(
                "timeliness",
                "iss_location",
                "iss_position_is_fresh",
                0 <= iss_age <= ISS_MAX_AGE_MINUTES,
                f"Position is {iss_age} min old (max {ISS_MAX_AGE_MINUTES} min).",
            )
        else:
            add_check(
                "timeliness",
                "iss_location",
                "iss_position_is_fresh",
                False,
                f"Invalid ISS timestamp: {iss_ts}",
            )
        observed_at = weather.get("observed_at")
        try:
            # Open-Meteo returns local time without an offset
            observed = pendulum.parse(observed_at, tz="America/Chicago")
            weather_age = (now - observed).in_minutes()
            add_check(
                "timeliness",
                "weather",
                "weather_observation_is_fresh",
                0 <= weather_age <= WEATHER_MAX_AGE_MINUTES,
                f"Observation is {weather_age} min old "
                f"(max {WEATHER_MAX_AGE_MINUTES} min).",
            )
        except Exception as err:  # noqa: BLE001 - report, don't crash
            add_check(
                "timeliness",
                "weather",
                "weather_observation_is_fresh",
                False,
                f"Could not parse observed_at={observed_at!r}: {err}",
            )

        # ---------------- Report ----------------
        failed = [c for c in checks if c["status"] == "FAIL"]
        by_dimension: dict[str, dict[str, int]] = {}
        for c in checks:
            dim = by_dimension.setdefault(c["dimension"], {"passed": 0, "failed": 0})
            dim["passed" if c["status"] == "PASS" else "failed"] += 1

        report = {
            "validated_at": now.to_iso8601_string(),
            "run_id": context["run_id"],
            "overall_status": "PASS" if not failed else "FAIL",
            "total_checks": len(checks),
            "passed_checks": len(checks) - len(failed),
            "failed_checks": len(failed),
            "by_dimension": by_dimension,
            "checks": checks,
        }

        print("=" * 60)
        print(f"DATA QUALITY VALIDATION REPORT - {report['validated_at']}")
        print("=" * 60)
        for c in checks:
            print(
                f"[{c['status']}] {c['dimension']:<12} {c['source']:<18} "
                f"{c['check']}: {c['details']}"
            )
        print("-" * 60)
        print(
            f"Overall: {report['overall_status']} "
            f"({report['passed_checks']}/{report['total_checks']} checks passed)"
        )
        print("=" * 60)
        return report

    @task
    def perform_statistical_methods(
        people_in_space: list[dict], astronaut_stats: dict
    ) -> dict:
        """
        This task performs advanced statistical methods on the astronaut
        data: percentile analysis, quartiles and interquartile range (IQR),
        z-scores, skewness and kurtosis, outlier detection (IQR fences and
        z-scores), and a Jarque-Bera normality test. It runs on astronaut
        name lengths, crew size per spacecraft, and capacity utilization.
        """

        def analyze(observations: list[tuple[str, float]]) -> dict:
            values = [value for _, value in observations]
            n = len(values)
            if n == 0:
                return {"count": 0}
            ordered = sorted(values)

            # Percentiles, quartiles and IQR
            percentiles = {
                f"p{p}": round(_percentile(ordered, p), 3) for p in PERCENTILES
            }
            q1, q2, q3 = (_percentile(ordered, p) for p in (25, 50, 75))
            iqr = q3 - q1
            lower_fence, upper_fence = q1 - 1.5 * iqr, q3 + 1.5 * iqr

            # Moments: z-scores, skewness and kurtosis
            moments = _moments(values)
            mean, std = moments["mean"], moments["std_dev"]
            z_scores = [
                {
                    "label": label,
                    "value": value,
                    "z_score": round((value - mean) / std, 3) if std else 0.0,
                }
                for label, value in observations
            ]

            # Outlier detection
            iqr_outliers = [
                z for z in z_scores if not lower_fence <= z["value"] <= upper_fence
            ]
            z_outliers = [
                z for z in z_scores if abs(z["z_score"]) > Z_SCORE_OUTLIER_THRESHOLD
            ]

            skew, kurt = moments["skewness"], moments["kurtosis"]
            if skew is None:
                shape = {"skew": "none (all values identical)", "tails": "n/a"}
            else:
                if abs(skew) < 0.5:
                    skew_label = "approximately symmetric"
                elif abs(skew) < 1:
                    skew_label = (
                        "moderately " + ("right" if skew > 0 else "left") + "-skewed"
                    )
                else:
                    skew_label = (
                        "highly " + ("right" if skew > 0 else "left") + "-skewed"
                    )
                if abs(kurt) < 0.5:
                    tails = "mesokurtic (normal-like tails)"
                elif kurt > 0:
                    tails = "leptokurtic (heavy tails)"
                else:
                    tails = "platykurtic (light tails)"
                shape = {"skew": skew_label, "tails": tails}

            # Distribution testing: Jarque-Bera, p-value from chi-square(2)
            if skew is None:
                normality = {"test": "jarque_bera", "status": "degenerate"}
            elif n < MIN_NORMALITY_SAMPLE:
                normality = {
                    "test": "jarque_bera",
                    "status": "insufficient_sample",
                    "details": f"n={n}, need at least {MIN_NORMALITY_SAMPLE}.",
                }
            else:
                jb = n / 6 * (skew**2 + kurt**2 / 4)
                p_value = math.exp(-jb / 2)
                normality = {
                    "test": "jarque_bera",
                    "status": "tested",
                    "statistic": round(jb, 4),
                    "p_value": round(p_value, 4),
                    "normal_at_5pct": p_value >= 0.05,
                    "note": "Asymptotic test; approximate for small samples.",
                }

            return {
                "count": n,
                "mean": round(mean, 3),
                "std_dev": round(std, 3),
                "percentiles": percentiles,
                "quartiles": {
                    "q1": round(q1, 3),
                    "q2": round(q2, 3),
                    "q3": round(q3, 3),
                },
                "iqr": round(iqr, 3),
                "iqr_fences": {
                    "lower": round(lower_fence, 3),
                    "upper": round(upper_fence, 3),
                },
                "skewness": round(skew, 4) if skew is not None else None,
                "kurtosis": round(kurt, 4) if kurt is not None else None,
                "shape": shape,
                "z_scores": z_scores,
                "outliers_iqr": iqr_outliers,
                "outliers_z": z_outliers,
                "normality": normality,
            }

        names = [str(p.get("name") or "").strip() for p in people_in_space]
        series = {
            "name_length": [(name, len(name)) for name in names if name],
            "crew_size_per_craft": [
                (craft, d["count"])
                for craft, d in astronaut_stats.get("distribution", {}).items()
            ],
            "capacity_utilization_pct": [
                (craft, m["utilization_pct"])
                for craft, m in astronaut_stats.get("capacity_metrics", {}).items()
                if m.get("utilization_pct") is not None
            ],
        }
        metrics = {name: analyze(obs) for name, obs in series.items()}

        result = {
            "metrics": metrics,
            "summary": {
                "metrics_analyzed": len(metrics),
                "total_iqr_outliers": sum(
                    len(m.get("outliers_iqr", [])) for m in metrics.values()
                ),
                "total_z_outliers": sum(
                    len(m.get("outliers_z", [])) for m in metrics.values()
                ),
            },
        }

        for name, m in metrics.items():
            if not m["count"]:
                print(f"{name}: no data")
                continue
            print(
                f"{name}: n={m['count']}, mean={m['mean']}, IQR={m['iqr']}, "
                f"skew={m['skewness']}, kurtosis={m['kurtosis']} ({m['shape']['skew']}), "
                f"outliers IQR={[o['label'] for o in m['outliers_iqr']]}, "
                f"z={[o['label'] for o in m['outliers_z']]}, "
                f"normality={m['normality']['status']}"
            )
        return result

    @task
    def calculate_trends(astronaut_stats: dict, weather: dict, **context) -> dict:
        """
        This task calculates trends, growth rates, and temporal patterns in
        astronaut data. It keeps a rolling history of key metrics by reading
        its own result from the previous run (XCom), then computes rate of
        change, moving averages, trend direction (least-squares slope),
        volatility, change anomalies, and a next-run forecast per metric.
        The first run has no history, so trends appear from the second run.
        """
        previous = context["ti"].xcom_pull(
            task_ids="calculate_trends", include_prior_dates=True
        )
        history = (
            list(previous.get("history", [])) if isinstance(previous, dict) else []
        )
        history = [h for h in history if h.get("run_id") != context["run_id"]]

        distribution = astronaut_stats.get("distribution", {})
        history.append(
            {
                "run_id": context["run_id"],
                "logical_date": context["ts"],
                "total_astronauts": astronaut_stats.get("total_astronauts", 0),
                "spacecraft_count": len(distribution),
                "mean_name_length": astronaut_stats.get("name_length_stats", {}).get(
                    "mean"
                ),
                "crew_by_craft": {c: d["count"] for c, d in distribution.items()},
                "temperature_c": weather.get("temperature_c"),
                "relative_humidity_pct": weather.get("relative_humidity_pct"),
                "surface_pressure_hpa": weather.get("surface_pressure_hpa"),
            }
        )
        history.sort(key=lambda h: h["logical_date"])
        history = history[-TREND_HISTORY_LENGTH:]

        def analyze_trend(values: list[float]) -> dict:
            n = len(values)
            result = {"periods": n, "current": values[-1]}
            if n < 2:
                result["direction"] = "insufficient_history"
                return result
            current, prior, first = values[-1], values[-2], values[0]
            change = current - prior
            result.update(
                {
                    "previous": prior,
                    "absolute_change": round(change, 3),
                    "rate_of_change_pct": round(change / prior * 100, 2)
                    if prior
                    else None,
                    "compound_growth_rate_pct": round(
                        ((current / first) ** (1 / (n - 1)) - 1) * 100, 3
                    )
                    if first > 0 and current > 0
                    else None,
                    "moving_averages": {
                        f"ma_{w}": round(statistics.mean(values[-w:]), 3)
                        for w in MOVING_AVERAGE_WINDOWS
                        if n >= w
                    },
                }
            )

            line = _linear_trend(values)
            level = statistics.mean(values)
            relative_slope = line["slope"] / abs(level) if level else line["slope"]
            if abs(relative_slope) < TREND_STABILITY_THRESHOLD:
                direction = "stable"
            else:
                direction = "increasing" if line["slope"] > 0 else "decreasing"

            changes = [b - a for a, b in zip(values, values[1:], strict=False)]
            is_anomalous, change_z = False, None
            if len(changes) >= 4:
                prior_changes = changes[:-1]
                m, s = statistics.mean(prior_changes), statistics.pstdev(prior_changes)
                if s:
                    change_z = round((changes[-1] - m) / s, 3)
                    is_anomalous = abs(change_z) > 2
                else:
                    is_anomalous = changes[-1] != m

            short_ma = statistics.mean(values[-MOVING_AVERAGE_WINDOWS[0] :])
            if line["r_squared"] >= 0.7 and n >= 7:
                confidence = "high"
            elif line["r_squared"] >= 0.4 and n >= 4:
                confidence = "medium"
            else:
                confidence = "low"

            result.update(
                {
                    "direction": direction,
                    "slope_per_run": round(line["slope"], 4),
                    "r_squared": round(line["r_squared"], 4),
                    "volatility": round(statistics.pstdev(changes), 4),
                    "momentum": "above_average"
                    if current > short_ma
                    else "below_average"
                    if current < short_ma
                    else "at_average",
                    "latest_change_z_score": change_z,
                    "is_anomalous": is_anomalous,
                    "forecast_next": round(line["intercept"] + line["slope"] * n, 2),
                    "forecast_confidence": confidence,
                }
            )
            return result

        series: dict[str, list[float]] = {
            key: [h[key] for h in history if h.get(key) is not None]
            for key in (
                "total_astronauts",
                "spacecraft_count",
                "mean_name_length",
                "temperature_c",
            )
        }
        crafts = sorted({c for h in history for c in h.get("crew_by_craft", {})})
        for craft in crafts:
            series[f"crew:{craft}"] = [
                h.get("crew_by_craft", {}).get(craft, 0) for h in history
            ]
        metrics = {
            key: analyze_trend(values) for key, values in series.items() if values
        }

        dates = [pendulum.parse(h["logical_date"]) for h in history]
        gaps = [
            (b - a).total_seconds() / 3600
            for a, b in zip(dates, dates[1:], strict=False)
        ]
        trends = {
            "periods_observed": len(history),
            "first_observation": history[0]["logical_date"],
            "latest_observation": history[-1]["logical_date"],
            "avg_interval_hours": round(statistics.mean(gaps), 2) if gaps else None,
            "metrics": metrics,
            "history": history,
        }

        print(
            f"Trend history: {len(history)} run(s) since {trends['first_observation']}"
        )
        for key, t in metrics.items():
            print(
                f"  - {key}: current={t['current']}, direction={t['direction']}, "
                f"change={t.get('absolute_change')}, "
                f"MAs={t.get('moving_averages')}, forecast={t.get('forecast_next')}"
            )
        return trends

    @task
    def perform_comprehensive_analysis(
        astronaut_stats: dict,
        statistical_analysis: dict,
        trends: dict,
        correlation: dict,
        data_quality: dict,
        weather: dict,
    ) -> dict:
        """
        This task performs comprehensive multi-dimensional analysis combining
        data from multiple sources (statistics, advanced statistical methods,
        trends, weather correlation and data quality). It provides trend
        analysis, anomaly detection, risk assessment (likelihood x impact),
        efficiency metrics, predictive insights, and strategic
        recommendations.
        """
        capacity_metrics = astronaut_stats.get("capacity_metrics", {})
        distribution = astronaut_stats.get("distribution", {})
        total = astronaut_stats.get("total_astronauts", 0)
        trend_metrics = trends.get("metrics", {})
        dq_checks = data_quality.get("checks", [])

        # ---------------- Trend analysis ----------------
        directions = {k: t.get("direction") for k, t in trend_metrics.items()}
        trend_analysis = {
            "periods_observed": trends.get("periods_observed", 0),
            "directions": directions,
            "increasing": sorted(k for k, d in directions.items() if d == "increasing"),
            "decreasing": sorted(k for k, d in directions.items() if d == "decreasing"),
            "stable": sorted(k for k, d in directions.items() if d == "stable"),
        }

        # ---------------- Anomaly detection ----------------
        anomalies: list[dict] = []
        for metric, a in statistical_analysis.get("metrics", {}).items():
            iqr_labels = {o["label"] for o in a.get("outliers_iqr", [])}
            for o in a.get("outliers_iqr", []):
                anomalies.append(
                    {
                        "type": "statistical_outlier",
                        "source": metric,
                        "severity": "medium",
                        "details": f"{o['label']}={o['value']} is outside the IQR "
                        f"fences {a['iqr_fences']} (z={o['z_score']}).",
                    }
                )
            for o in a.get("outliers_z", []):
                if o["label"] not in iqr_labels:
                    anomalies.append(
                        {
                            "type": "statistical_outlier",
                            "source": metric,
                            "severity": "low",
                            "details": f"{o['label']}={o['value']} has z-score {o['z_score']}.",
                        }
                    )
        for metric, t in trend_metrics.items():
            if t.get("is_anomalous"):
                anomalies.append(
                    {
                        "type": "trend_break",
                        "source": metric,
                        "severity": "medium",
                        "details": f"Latest change of {t.get('absolute_change')} breaks the "
                        f"historical pattern (z={t.get('latest_change_z_score')}).",
                    }
                )
        for c in dq_checks:
            if c["status"] == "FAIL":
                anomalies.append(
                    {
                        "type": "data_quality",
                        "source": f"{c['source']}.{c['check']}",
                        "severity": "high"
                        if c["dimension"] in ("completeness", "accuracy")
                        else "medium",
                        "details": c["details"],
                    }
                )

        # ---------------- Risk assessment ----------------
        risks: list[dict] = []

        def add_risk(
            risk_id: str, area: str, likelihood: int, impact: int, details: str
        ):
            score = likelihood * impact
            level = "high" if score >= 15 else "medium" if score >= 8 else "low"
            risks.append(
                {
                    "id": risk_id,
                    "area": area,
                    "likelihood": likelihood,
                    "impact": impact,
                    "score": score,
                    "level": level,
                    "details": details,
                }
            )

        utilizations = {
            c: m["utilization_pct"]
            for c, m in capacity_metrics.items()
            if m.get("utilization_pct") is not None
        }
        if utilizations:
            craft, peak = max(utilizations.items(), key=lambda i: i[1])
            likelihood = 5 if peak > 100 else 3 if peak >= 85 else 1
            add_risk(
                "crew_capacity",
                "operations",
                likelihood,
                4,
                f"Peak utilization is {peak}% on {craft}.",
            )
        projected_over = []
        for craft, m in capacity_metrics.items():
            t = trend_metrics.get(f"crew:{craft}", {})
            capacity = m.get("nominal_capacity")
            if (
                capacity
                and t.get("forecast_next") is not None
                and t["forecast_next"] > capacity
            ):
                projected_over.append(f"{craft} ({t['forecast_next']} vs {capacity})")
        if projected_over:
            add_risk(
                "projected_capacity",
                "operations",
                3,
                4,
                f"Forecast exceeds nominal capacity: {', '.join(projected_over)}.",
            )
        total_checks = data_quality.get("total_checks", 0) or 0
        failed_checks = data_quality.get("failed_checks", 0) or 0
        fail_ratio = failed_checks / total_checks if total_checks else 1.0
        add_risk(
            "data_quality",
            "data",
            min(5, 1 + round(fail_ratio * 8)),
            4,
            f"{failed_checks}/{total_checks} data quality checks failed.",
        )
        shares = [d["count"] / total for d in distribution.values()] if total else []
        hhi = round(sum(s**2 for s in shares), 4) if shares else None
        if hhi is not None:
            add_risk(
                "crew_concentration",
                "operations",
                1 + round(hhi * 4),
                2,
                f"Crew concentration index (HHI) is {hhi} (1.0 = all crew on one craft).",
            )
        untracked = sorted(
            c for c, m in capacity_metrics.items() if m.get("nominal_capacity") is None
        )
        if untracked:
            add_risk(
                "capacity_visibility",
                "governance",
                3,
                2,
                f"No nominal capacity defined for: {', '.join(untracked)}.",
            )
        wind = weather.get("wind_speed_kmh") or 0
        rain = weather.get("precipitation_mm") or 0
        add_risk(
            "ground_weather",
            "ground operations",
            4 if wind >= 60 else 3 if wind >= 40 or rain > 0 else 1,
            2,
            f"Wind {wind} km/h, precipitation {rain} mm at JSC.",
        )
        risks.sort(key=lambda r: r["score"], reverse=True)
        overall_risk = risks[0]["level"] if risks else "low"

        # ---------------- Efficiency metrics ----------------
        known_capacity = sum(
            m["nominal_capacity"]
            for m in capacity_metrics.values()
            if m.get("nominal_capacity")
        )
        crew_on_known = sum(
            m["crew"] for m in capacity_metrics.values() if m.get("nominal_capacity")
        )
        efficiency = {
            "tracked_capacity": known_capacity,
            "crew_on_tracked_crafts": crew_on_known,
            "overall_utilization_pct": round(crew_on_known / known_capacity * 100, 2)
            if known_capacity
            else None,
            "spare_seats": known_capacity - crew_on_known,
            "utilization_by_craft_pct": utilizations,
            "crew_concentration_hhi": hhi,
            "untracked_crafts": untracked,
            "data_quality_pass_rate_pct": round((1 - fail_ratio) * 100, 2),
        }

        # ---------------- Predictive insights ----------------
        predictive = []
        for key, t in trend_metrics.items():
            if t.get("forecast_next") is not None:
                predictive.append(
                    {
                        "metric": key,
                        "current": t["current"],
                        "forecast_next": t["forecast_next"],
                        "direction": t["direction"],
                        "confidence": t["forecast_confidence"],
                    }
                )
        if not predictive:
            predictive.append(
                {
                    "metric": "all",
                    "details": "Not enough history yet; forecasts start after the next run.",
                }
            )

        # ---------------- Strategic recommendations ----------------
        playbook = {
            "crew_capacity": "Review crew rotation and life-support margins on the busiest craft.",
            "projected_capacity": "Plan crew rotations ahead of the projected capacity breach.",
            "data_quality": "Fix failing data quality checks before relying on downstream reports.",
            "crew_concentration": "Track dependency on a single station; prepare contingency plans.",
            "capacity_visibility": "Add the missing crafts to SPACECRAFT_CAPACITY to track utilization.",
            "ground_weather": "Reschedule weather-sensitive ground operations at JSC.",
        }
        recommendations = [
            {
                "priority": r["level"],
                "area": r["area"],
                "recommendation": playbook[r["id"]],
                "rationale": r["details"],
            }
            for r in risks
            if r["level"] in ("high", "medium")
        ]
        if trend_analysis["periods_observed"] < MOVING_AVERAGE_WINDOWS[-1]:
            recommendations.append(
                {
                    "priority": "low",
                    "area": "analytics",
                    "recommendation": "Keep the DAG running on schedule to build trend history.",
                    "rationale": f"Only {trend_analysis['periods_observed']} run(s) of history; "
                    f"{MOVING_AVERAGE_WINDOWS[-1]} are needed for the long moving average.",
                }
            )
        if untracked and not any(
            r["recommendation"] == playbook["capacity_visibility"]
            for r in recommendations
        ):
            recommendations.append(
                {
                    "priority": "low",
                    "area": "governance",
                    "recommendation": playbook["capacity_visibility"],
                    "rationale": f"Untracked crafts: {', '.join(untracked)}.",
                }
            )
        if not recommendations:
            recommendations.append(
                {
                    "priority": "low",
                    "area": "operations",
                    "recommendation": "Maintain current operations; all indicators are nominal.",
                    "rationale": "No medium or high risks detected.",
                }
            )

        analysis = {
            "total_astronauts": total,
            "calm_and_clear_at_jsc": correlation.get("indicators", {}).get(
                "calm_and_clear_at_jsc"
            ),
            "trend_analysis": trend_analysis,
            "anomalies": anomalies,
            "risk_assessment": {"overall_risk": overall_risk, "risks": risks},
            "efficiency_metrics": efficiency,
            "predictive_insights": predictive,
            "strategic_recommendations": recommendations,
        }

        print(f"Overall risk: {overall_risk}; {len(anomalies)} anomaly(ies) detected.")
        for r in risks:
            print(
                f"  - [{r['level'].upper()}] {r['id']} (score {r['score']}): {r['details']}"
            )
        print(f"Efficiency: {efficiency}")
        for rec in recommendations:
            print(f"  - ({rec['priority']}) {rec['recommendation']}")
        return analysis

    @task
    def generate_insights_report(
        astronaut_stats: dict,
        statistical_analysis: dict,
        trends: dict,
        correlation: dict,
        data_quality: dict,
        comprehensive_analysis: dict,
        summary_report: dict,
        **context,
    ) -> dict:
        """
        This task generates an executive insights report synthesizing all
        analysis outputs (astronaut statistics, advanced statistical methods,
        trends, weather correlation, data quality, comprehensive analysis and
        the summary report) into actionable business intelligence. It
        provides key findings, critical alerts, opportunity identification,
        and decision support recommendations.
        """
        now = pendulum.now("UTC")
        risk_assessment = comprehensive_analysis.get("risk_assessment", {})
        risks = risk_assessment.get("risks", [])
        overall_risk = risk_assessment.get("overall_risk", "unknown")
        anomalies = comprehensive_analysis.get("anomalies", [])
        efficiency = comprehensive_analysis.get("efficiency_metrics", {})
        predictive = comprehensive_analysis.get("predictive_insights", [])
        trend_metrics = trends.get("metrics", {})
        stat_metrics = statistical_analysis.get("metrics", {})
        indicators = correlation.get("indicators", {})
        total = astronaut_stats.get("total_astronauts", 0)
        distribution = astronaut_stats.get("distribution", {})

        # ---------------- Key findings ----------------
        # One or more findings per analysis output, tagged with its source.
        key_findings: list[dict] = []

        def finding(source: str, text: str) -> None:
            key_findings.append({"source": source, "finding": text})

        finding(
            "astronaut_stats",
            f"{total} astronaut(s) in space across {len(distribution)} spacecraft"
            + (
                f"; {next(iter(distribution))} carries "
                f"{next(iter(distribution.values()))['share_pct']}% of the crew."
                if distribution
                else "."
            ),
        )
        if efficiency.get("overall_utilization_pct") is not None:
            finding(
                "comprehensive_analysis",
                f"Tracked spacecraft are {efficiency['overall_utilization_pct']}% "
                f"utilized with {efficiency.get('spare_seats')} spare seat(s); "
                f"overall risk is {overall_risk}.",
            )
        for metric, m in stat_metrics.items():
            if not m.get("count"):
                continue
            outliers = sorted(
                {
                    o["label"]
                    for o in m.get("outliers_iqr", []) + m.get("outliers_z", [])
                }
            )
            normality = m.get("normality", {})
            normal_text = (
                f"normal at 5% (p={normality['p_value']})"
                if normality.get("normal_at_5pct")
                else f"not normal at 5% (p={normality['p_value']})"
                if normality.get("status") == "tested"
                else f"normality {normality.get('status', 'not tested')}"
            )
            finding(
                "statistical_analysis",
                f"{metric}: median {m['quartiles']['q2']}, IQR {m['iqr']}, "
                f"{m['shape']['skew']}; {normal_text}; "
                + (f"outliers: {', '.join(outliers)}." if outliers else "no outliers."),
            )
        if trends.get("periods_observed", 0) < 2:
            finding(
                "trends",
                "First run with trend tracking; trends appear from the next run.",
            )
        else:
            moving = [
                f"{k} {t['direction']} ({t['absolute_change']:+} vs last run)"
                for k, t in trend_metrics.items()
                if t.get("direction") in ("increasing", "decreasing")
            ]
            finding(
                "trends",
                f"Over {trends['periods_observed']} runs: "
                + ("; ".join(moving) if moving else "all tracked metrics are stable")
                + ".",
            )
        weather_snapshot = correlation.get("weather", {})
        finding(
            "correlation",
            f"Houston (JSC): {weather_snapshot.get('temperature_c')}°C, wind "
            f"{weather_snapshot.get('wind_speed_kmh')} km/h; calm and clear: "
            f"{indicators.get('calm_and_clear_at_jsc')}.",
        )
        finding(
            "data_quality",
            f"Data quality {data_quality.get('overall_status')}: "
            f"{data_quality.get('passed_checks')}/{data_quality.get('total_checks')} "
            "checks passed.",
        )

        # ---------------- Critical alerts ----------------
        # Critical: high risks and high-severity anomalies. Warning: medium.
        severity_rank = {"critical": 0, "warning": 1}
        level_to_severity = {"high": "critical", "medium": "warning"}
        critical_alerts = [
            {
                "severity": level_to_severity[r["level"]],
                "source": "risk_assessment",
                "title": r["id"].replace("_", " ").capitalize(),
                "details": r["details"],
                "risk_score": r["score"],
            }
            for r in risks
            if r["level"] in level_to_severity
        ] + [
            {
                "severity": level_to_severity[a["severity"]],
                "source": a["type"],
                "title": a["source"],
                "details": a["details"],
            }
            for a in anomalies
            if a["severity"] in level_to_severity
        ]
        critical_alerts.sort(key=lambda a: severity_rank[a["severity"]])

        # ---------------- Opportunities ----------------
        opportunities: list[dict] = []
        if (efficiency.get("spare_seats") or 0) > 0:
            opportunities.append(
                {
                    "opportunity": "Use spare crew capacity for additional crew "
                    "or visiting missions.",
                    "basis": f"{efficiency['spare_seats']} spare seat(s); "
                    f"{efficiency.get('overall_utilization_pct')}% utilized.",
                }
            )
        if indicators.get("calm_and_clear_at_jsc"):
            opportunities.append(
                {
                    "opportunity": "Schedule outdoor operations and public "
                    "ISS-observation events at JSC.",
                    "basis": "Calm, clear conditions in Houston.",
                }
            )
        if data_quality.get("overall_status") == "PASS":
            opportunities.append(
                {
                    "opportunity": "Feed this run's data to downstream dashboards "
                    "and automation.",
                    "basis": "All data quality checks passed.",
                }
            )
        confident = [p for p in predictive if p.get("confidence") in ("high", "medium")]
        if confident:
            opportunities.append(
                {
                    "opportunity": "Use forecasts for crew and capacity planning.",
                    "basis": "; ".join(
                        f"{p['metric']} -> {p['forecast_next']} "
                        f"({p['confidence']} confidence)"
                        for p in confident
                    ),
                }
            )
        low_risk_areas = sorted({r["area"] for r in risks if r["level"] == "low"})
        if low_risk_areas:
            opportunities.append(
                {
                    "opportunity": "Reduce monitoring effort on stable, low-risk "
                    "areas and redirect it to open alerts.",
                    "basis": f"Low risk in: {', '.join(low_risk_areas)}.",
                }
            )

        # ---------------- Decision support ----------------
        decision_support = []
        for rec in comprehensive_analysis.get("strategic_recommendations", []):
            days = DECISION_WINDOW_DAYS[rec["priority"]]
            evidence = [
                a["details"]
                for a in critical_alerts
                if a["source"] == "risk_assessment" and a["details"] == rec["rationale"]
            ] or [rec["rationale"]]
            decision_support.append(
                {
                    "priority": rec["priority"],
                    "area": rec["area"],
                    "recommendation": rec["recommendation"],
                    "evidence": evidence,
                    "decide_within_days": days,
                    "due_by": now.add(days=days).to_date_string(),
                }
            )
        decision_support.sort(
            key=lambda d: list(DECISION_WINDOW_DAYS).index(d["priority"])
        )

        critical_count = sum(1 for a in critical_alerts if a["severity"] == "critical")
        headline = (
            f"{total} astronauts across {len(distribution)} spacecraft. "
            f"Overall risk {overall_risk}; {critical_count} critical and "
            f"{len(critical_alerts) - critical_count} warning alert(s); "
            f"{len(opportunities)} opportunity(ies); "
            f"{len(decision_support)} decision(s) pending."
        )

        report = {
            "generated_at": now.to_iso8601_string(),
            "report_date": summary_report.get("report_date", context["ds"]),
            "run_id": context["run_id"],
            "headline": headline,
            "key_findings": key_findings,
            "critical_alerts": critical_alerts,
            "opportunities": opportunities,
            "decision_support": decision_support,
        }

        print("=" * 60)
        print(f"EXECUTIVE INSIGHTS REPORT - {report['report_date']}")
        print("=" * 60)
        print(headline)
        print("Key findings:")
        for f in key_findings:
            print(f"  - [{f['source']}] {f['finding']}")
        print("Critical alerts:")
        for a in critical_alerts or [{"severity": "none", "title": "-", "details": ""}]:
            print(f"  [{a['severity'].upper()}] {a['title']}: {a['details']}")
        print("Opportunities:")
        for o in opportunities or [{"opportunity": "none identified", "basis": "-"}]:
            print(f"  - {o['opportunity']} ({o['basis']})")
        print("Decision support:")
        for d in decision_support:
            print(
                f"  [{d['priority'].upper()}] by {d['due_by']}: {d['recommendation']}"
            )
        print("=" * 60)
        return report

    @task(
        # Updates the `regression_predictions` Dataset on every successful run
        outlets=[regression_predictions_dataset]
    )
    def build_regression_model(
        astronaut_stats: dict, trends: dict, weather: dict, **context
    ) -> dict:
        """
        This task builds a simple linear regression model to predict
        astronaut counts based on historical patterns and weather
        correlations. It uses the least squares method to fit a trend line
        and generates predictions.

        - Linear regression (least squares): slope and intercept of the
          trend line, trained on REGRESSION_PERIODS periods. Real runs from
          the `calculate_trends` history are used where available; earlier
          periods are synthetic data generated from the current metrics and
          trend (seeded by the run date, so reruns are reproducible).
        - Model accuracy: R squared, correlation coefficient, standard error
          of the estimate and slope, and a fit quality rating
          (Excellent/Good/Moderate/Poor).
        - Future predictions: the next PREDICTION_HORIZON_PERIODS periods with
          95% confidence intervals (and prediction intervals), adjusted for
          weather and momentum.
        - Feature importance: contribution of trend growth, momentum,
          temperature, humidity, pressure and capacity utilization, shown as
          a bar chart in the log.
        - Model insights: trend direction, predictive accuracy, top
          influencing factors, and population growth/decline alerts.
        """
        weather_keys = (
            "temperature_c",
            "relative_humidity_pct",
            "surface_pressure_hpa",
        )
        rng = random.Random(context["ds"])

        # ---------------- Training data (observed + synthetic) ----------------
        trend_total = trends.get("metrics", {}).get("total_astronauts", {})
        slope_hint = trend_total.get("slope_per_run") or 0.0
        noise_sd = trend_total.get("volatility") or SYNTHETIC_NOISE_SD
        current_count = astronaut_stats.get("total_astronauts", 0)

        observed = [
            h
            for h in trends.get("history", [])
            if h.get("total_astronauts") is not None
        ][-REGRESSION_PERIODS:]
        if not observed:
            observed = [
                {
                    "total_astronauts": current_count,
                    **{key: weather.get(key) for key in weather_keys},
                }
            ]
        anchor = observed[0]

        def weather_value(source: dict, key: str):
            value = source.get(key)
            return value if value is not None else weather.get(key)

        periods: list[dict] = []
        synthetic_count = REGRESSION_PERIODS - len(observed)
        for k in range(synthetic_count, 0, -1):
            # k periods before the earliest observed run, walking the trend back
            count = anchor["total_astronauts"] - slope_hint * k + rng.gauss(0, noise_sd)
            period = {"source": "synthetic", "astronauts": max(0, round(count))}
            for key in weather_keys:
                base = weather_value(anchor, key)
                if base is None:
                    period[key] = None
                    continue
                value = base + rng.gauss(0, SYNTHETIC_WEATHER_SD[key])
                if key == "relative_humidity_pct":
                    value = min(100.0, max(0.0, value))
                period[key] = round(value, 2)
            periods.append(period)
        for h in observed:
            periods.append(
                {
                    "source": "observed",
                    "astronauts": h["total_astronauts"],
                    **{key: weather_value(h, key) for key in weather_keys},
                }
            )

        # ---------------- Least squares trend line ----------------
        n = len(periods)
        xs = list(range(n))
        ys = [p["astronauts"] for p in periods]
        x_mean, y_mean = (n - 1) / 2, sum(ys) / n
        sxx = sum((x - x_mean) ** 2 for x in xs)
        sxy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=False))
        syy = sum((y - y_mean) ** 2 for y in ys)
        slope = sxy / sxx
        intercept = y_mean - slope * x_mean
        fitted = [intercept + slope * x for x in xs]
        residuals = [y - f for y, f in zip(ys, fitted, strict=False)]
        ss_res = sum(r**2 for r in residuals)

        # ---------------- Accuracy metrics ----------------
        r_squared = 1 - ss_res / syy if syy else 1.0
        correlation_r = sxy / math.sqrt(sxx * syy) if syy else 0.0
        dof = n - 2
        std_error = math.sqrt(ss_res / dof) if dof > 0 else 0.0
        se_slope = std_error / math.sqrt(sxx)
        se_intercept = std_error * math.sqrt(1 / n + x_mean**2 / sxx)
        t_value = _t_critical_95(dof)
        positive = [(y, r) for y, r in zip(ys, residuals, strict=False) if y > 0]
        mape = (
            sum(abs(r) / y for y, r in positive) / len(positive) * 100
            if positive
            else None
        )
        fit_quality = next(
            (label for floor, label in FIT_QUALITY_BANDS if r_squared >= floor), "Poor"
        )

        # ---------------- Feature importance ----------------
        capacity = sum(
            m["nominal_capacity"]
            for m in astronaut_stats.get("capacity_metrics", {}).values()
            if m.get("nominal_capacity")
        ) or sum(SPACECRAFT_CAPACITY.values())
        # Lagged momentum and utilization, so features only use information
        # available before the period they explain.
        feature_rows = [
            {
                "target": ys[i],
                "trend_growth": i,
                "momentum": ys[i - 1] - ys[i - 2],
                "temperature": periods[i]["temperature_c"],
                "humidity": periods[i]["relative_humidity_pct"],
                "pressure": periods[i]["surface_pressure_hpa"],
                "capacity_utilization": ys[i - 1] / capacity * 100,
            }
            for i in range(2, n)
        ]
        feature_names = (
            "trend_growth",
            "momentum",
            "temperature",
            "humidity",
            "pressure",
            "capacity_utilization",
        )
        correlations = {}
        for name in feature_names:
            pairs = [
                (row[name], row["target"])
                for row in feature_rows
                if row[name] is not None
            ]
            fit = (
                _least_squares([p[0] for p in pairs], [p[1] for p in pairs])
                if len(pairs) >= 3
                else None
            )
            correlations[name] = fit["pearson_r"] if fit else 0.0
        total_abs = sum(abs(r) for r in correlations.values())
        feature_importance = sorted(
            (
                {
                    "feature": name,
                    "correlation": round(r, 4),
                    "effect": "positive" if r > 0 else "negative" if r < 0 else "none",
                    "importance_pct": round(abs(r) / total_abs * 100, 2)
                    if total_abs
                    else 0.0,
                }
                for name, r in correlations.items()
            ),
            key=lambda f: f["importance_pct"],
            reverse=True,
        )

        # ---------------- Prediction adjustments ----------------
        # Weather: residuals of the trend line regressed on each weather
        # variable, applied to today's reading (carried forward), capped at
        # one standard error so weather can only nudge the trend.
        weather_adjustment = 0.0
        weather_effects = {}
        for key in weather_keys:
            pairs = [
                (p[key], r)
                for p, r in zip(periods, residuals, strict=False)
                if p[key] is not None
            ]
            fit = (
                _least_squares([p[0] for p in pairs], [p[1] for p in pairs])
                if len(pairs) >= 3
                else None
            )
            current = weather.get(key)
            if fit and current is not None:
                mean_value = sum(p[0] for p in pairs) / len(pairs)
                effect = fit["slope"] * (current - mean_value)
                weather_effects[key] = round(effect, 4)
                weather_adjustment += effect
        weather_adjustment = max(-std_error, min(std_error, weather_adjustment))
        # Momentum: how far the latest change departs from the trend slope,
        # decaying geometrically with each period ahead.
        latest_change = ys[-1] - ys[-2] if n >= 2 else 0
        excess_momentum = latest_change - slope

        # ---------------- Future predictions ----------------
        predictions = []
        momentum_carry = 0.0
        for step in range(1, PREDICTION_HORIZON_PERIODS + 1):
            x0 = n - 1 + step
            base = intercept + slope * x0
            momentum_carry += excess_momentum * MOMENTUM_DECAY**step
            value = max(0.0, base + weather_adjustment + momentum_carry)
            leverage = 1 / n + (x0 - x_mean) ** 2 / sxx
            ci = t_value * std_error * math.sqrt(leverage)
            pi = t_value * std_error * math.sqrt(1 + leverage)
            predictions.append(
                {
                    "period_ahead": step,
                    "trend_prediction": round(base, 2),
                    "weather_adjustment": round(weather_adjustment, 3),
                    "momentum_adjustment": round(momentum_carry, 3),
                    "predicted_astronauts": round(value, 2),
                    "predicted_astronauts_rounded": round(value),
                    "confidence_interval_95": {
                        "low": round(max(0.0, value - ci), 2),
                        "high": round(value + ci, 2),
                    },
                    "prediction_interval_95": {
                        "low": round(max(0.0, value - pi), 2),
                        "high": round(value + pi, 2),
                    },
                    # Shorthand for the dashboard: the 95% confidence interval
                    "interval_low": round(max(0.0, value - ci), 2),
                    "interval_high": round(value + ci, 2),
                }
            )

        # ---------------- Model insights ----------------
        slope_low, slope_high = slope - t_value * se_slope, slope + t_value * se_slope
        significant = slope_low > 0 or slope_high < 0
        relative_slope = slope / y_mean if y_mean else slope
        if abs(relative_slope) < TREND_STABILITY_THRESHOLD or not significant:
            direction = "Stable"
        else:
            direction = "Increasing" if slope > 0 else "Decreasing"

        relative_se = std_error / y_mean * 100 if y_mean else None
        if relative_se is None:
            accuracy_level = "Unknown"
        elif relative_se <= 5:
            accuracy_level = "High"
        elif relative_se <= 15:
            accuracy_level = "Medium"
        else:
            accuracy_level = "Low"
        accuracy_summary = (
            f"{accuracy_level} predictive accuracy: typical error is "
            f"+/-{std_error:.2f} astronauts ({relative_se:.1f}% of the mean). "
            if relative_se is not None
            else "Predictive accuracy unknown (mean count is zero). "
        ) + (
            f"Fit quality is {fit_quality} (R²={r_squared:.3f})"
            + (
                "; a low R² with a small error means the count is flat, not "
                "that predictions are poor."
                if fit_quality in ("Moderate", "Poor") and accuracy_level == "High"
                else "."
            )
        )

        top_factors = [
            f"{f['feature']} ({f['importance_pct']}%, {f['effect']})"
            for f in feature_importance[:3]
            if f["importance_pct"] > 0
        ]

        alerts = []
        final = predictions[-1]
        if current_count:
            change_pct = (
                (final["predicted_astronauts"] - current_count) / current_count * 100
            )
            if change_pct >= POPULATION_ALERT_PCT:
                alerts.append(
                    f"POPULATION GROWTH: {final['predicted_astronauts']} astronauts "
                    f"predicted in {PREDICTION_HORIZON_PERIODS} periods "
                    f"(+{change_pct:.1f}% vs {current_count} today)."
                )
            elif change_pct <= -POPULATION_ALERT_PCT:
                alerts.append(
                    f"POPULATION DECLINE: {final['predicted_astronauts']} astronauts "
                    f"predicted in {PREDICTION_HORIZON_PERIODS} periods "
                    f"({change_pct:.1f}% vs {current_count} today)."
                )
        else:
            change_pct = None
        over_capacity = [
            p for p in predictions if p["predicted_astronauts_rounded"] > capacity
        ]
        if over_capacity:
            alerts.append(
                f"CAPACITY: predicted crew exceeds nominal capacity ({capacity}) "
                f"from period +{over_capacity[0]['period_ahead']}."
            )

        observed_count = sum(1 for p in periods if p["source"] == "observed")
        result = {
            "status": "fitted",
            "method": "ordinary_least_squares",
            "target": "total_astronauts",
            "training_data": {
                "periods": n,
                "observed_periods": observed_count,
                "synthetic_periods": n - observed_count,
                "synthetic_seed": context["ds"],
                "note": "Synthetic periods are generated from the current metrics "
                "and trend; they are not real observations.",
            },
            # The training series, so downstream model comparison and
            # cross-validation use exactly the same data.
            "series": [
                {"period": x, "astronauts": y, "source": p["source"]}
                for x, y, p in zip(xs, ys, periods, strict=False)
            ],
            "model": {
                "slope": round(slope, 4),
                "intercept": round(intercept, 4),
                "equation": f"astronauts = {intercept:.4f} + {slope:.4f} * period",
                "slope_ci_95": [round(slope_low, 4), round(slope_high, 4)],
            },
            "accuracy": {
                "r_squared": round(r_squared, 4),
                "correlation_coefficient": round(correlation_r, 4),
                "standard_error_estimate": round(std_error, 4),
                "standard_error_slope": round(se_slope, 4),
                "standard_error_intercept": round(se_intercept, 4),
                "mape_pct": round(mape, 2) if mape is not None else None,
                "degrees_of_freedom": dof,
                "fit_quality": fit_quality,
            },
            "adjustments": {
                "weather_effects": weather_effects,
                "weather_adjustment": round(weather_adjustment, 4),
                "latest_change": latest_change,
                "excess_momentum": round(excess_momentum, 4),
                "momentum_decay": MOMENTUM_DECAY,
            },
            "predictions": predictions,
            "feature_importance": feature_importance,
            "insights": {
                "trend_direction": direction,
                "trend_significant_at_95pct": significant,
                "predictive_accuracy": {
                    "level": accuracy_level,
                    "relative_standard_error_pct": round(relative_se, 2)
                    if relative_se is not None
                    else None,
                    "summary": accuracy_summary,
                },
                "top_factors": top_factors,
                "population_change_pct": round(change_pct, 2)
                if change_pct is not None
                else None,
                "population_alerts": alerts,
            },
        }

        # ---------------- Log output ----------------
        width = 72
        print("=" * width)
        print("ASTRONAUT COUNT REGRESSION MODEL (least squares)")
        print("=" * width)
        print(
            f"Training data: {n} periods ({observed_count} observed, "
            f"{n - observed_count} synthetic, seed {context['ds']})"
        )
        print(f"Trend line: {result['model']['equation']}")
        print(
            f"Accuracy: R²={r_squared:.4f}, r={correlation_r:.4f}, "
            f"SE={std_error:.4f}, SE(slope)={se_slope:.4f}, fit={fit_quality}"
        )
        print("-" * width)
        print(f"PREDICTIONS (next {PREDICTION_HORIZON_PERIODS} periods, 95% CI)")
        for p in predictions:
            ci = p["confidence_interval_95"]
            print(
                f"  +{p['period_ahead']}: {p['predicted_astronauts']:>6} "
                f"[{ci['low']} - {ci['high']}]  trend {p['trend_prediction']}, "
                f"weather {p['weather_adjustment']:+}, momentum {p['momentum_adjustment']:+}"
            )
        print("-" * width)
        print("FEATURE IMPORTANCE")
        for f in feature_importance:
            bar = "#" * round(f["importance_pct"] / 100 * 40)
            print(
                f"  {f['feature']:<21} {bar:<40} {f['importance_pct']:>6}% ({f['effect']})"
            )
        print("-" * width)
        print("INSIGHTS")
        print(f"  Trend direction: {direction} (significant: {significant})")
        print(f"  {accuracy_summary}")
        print(f"  Top factors: {', '.join(top_factors) or 'none'}")
        for alert in alerts or ["No population growth/decline alerts."]:
            print(f"  ALERT: {alert}" if alerts else f"  {alert}")
        print("=" * width)
        return result

    @task(
        # Updates the `model_comparison` Dataset on every successful run
        outlets=[model_comparison_dataset]
    )
    def compare_regression_models(
        people_in_space: list[dict],
        astronaut_stats: dict,
        trends: dict,
        weather: dict,
        regression_model: dict,
    ) -> dict:
        """
        This task compares multiple regression models (Linear, Polynomial
        degree 2, Polynomial degree 3) and evaluates their performance using
        R squared, adjusted R squared, RMSE, MAE, and AIC to determine the
        best model.

        Polynomials are fitted with the normal equations and a Gaussian
        elimination solver (no external libraries), on the same training
        series as `build_regression_model`. Models are ranked by a composite
        score (adjusted R squared 40%, RMSE 30%, AIC 30%), given medals, and
        the winner comes with contextual recommendations and overfitting
        warnings for the more complex models.
        """
        series = regression_model.get("series", [])
        xs = [p["period"] for p in series]
        ys = [p["astronauts"] for p in series]
        n = len(series)
        current_count = astronaut_stats.get("total_astronauts", len(people_in_space))
        result = {
            "training_periods": n,
            "observed_periods": sum(1 for p in series if p["source"] == "observed"),
            "real_runs_in_trend_history": trends.get("periods_observed", 0),
            "current_astronauts": current_count,
            "current_temperature_c": weather.get("temperature_c"),
        }
        if n < COMPARISON_MODELS[-1][1] + 3:
            result.update(
                {"status": "insufficient_data", "models": [], "best_model": None}
            )
            print(f"Only {n} training periods; not enough to compare models.")
            return result

        # ---------------- Fit and score each model ----------------
        models = []
        for name, degree in COMPARISON_MODELS:
            fit = _fit_polynomial(xs, ys, degree)
            if fit is None:
                print(f"{name}: normal equations are singular; skipped.")
                continue
            params = degree + 1
            fitted = [_predict_polynomial(fit, x) for x in xs]
            sse = sum((y - f) ** 2 for y, f in zip(ys, fitted, strict=False))
            metrics = _error_metrics(ys, fitted)
            r2 = metrics["r_squared"] if metrics["r_squared"] is not None else 1.0
            adj_r2 = 1 - (1 - r2) * (n - 1) / (n - params) if n > params else r2
            aic = n * math.log(max(sse / n, 1e-12)) + 2 * params
            models.append(
                {
                    "model": name,
                    "degree": degree,
                    "parameters": params,
                    "equation": _polynomial_equation(fit),
                    "fit": fit,
                    "metrics": {
                        "r_squared": round(r2, 4),
                        "adjusted_r_squared": round(adj_r2, 4),
                        "rmse": metrics["rmse"],
                        "mae": metrics["mae"],
                        "aic": round(aic, 3),
                    },
                    "forecast": [
                        round(max(0.0, _predict_polynomial(fit, n - 1 + h)), 2)
                        for h in range(1, PREDICTION_HORIZON_PERIODS + 1)
                    ],
                }
            )

        if not models:
            result.update({"status": "no_model", "models": [], "best_model": None})
            print("No model could be fitted.")
            return result

        # ---------------- Composite score and ranking ----------------
        def normalize(values: list[float], higher_is_better: bool) -> list[float]:
            low, high = min(values), max(values)
            if high - low < 1e-12:
                return [1.0] * len(values)
            return [
                (v - low) / (high - low)
                if higher_is_better
                else (high - v) / (high - low)
                for v in values
            ]

        adj_scores = normalize(
            [m["metrics"]["adjusted_r_squared"] for m in models], True
        )
        rmse_scores = normalize([m["metrics"]["rmse"] for m in models], False)
        aic_scores = normalize([m["metrics"]["aic"] for m in models], False)
        for m, a, r, c in zip(
            models, adj_scores, rmse_scores, aic_scores, strict=False
        ):
            m["composite_score"] = round(
                100
                * (
                    COMPOSITE_WEIGHTS["adjusted_r_squared"] * a
                    + COMPOSITE_WEIGHTS["rmse"] * r
                    + COMPOSITE_WEIGHTS["aic"] * c
                ),
                2,
            )
        # Highest score wins; on a tie the simpler model ranks higher.
        models.sort(key=lambda m: (-m["composite_score"], m["degree"]))
        for rank, m in enumerate(models, start=1):
            m["rank"] = rank
            m["medal"] = MEDALS[rank - 1] if rank <= len(MEDALS) else None

        # ---------------- Overfitting warnings ----------------
        linear = next((m for m in models if m["degree"] == 1), None)
        for m in models:
            warnings = []
            if m["degree"] > 1 and linear:
                metrics, base = m["metrics"], linear["metrics"]
                if metrics["r_squared"] - metrics["adjusted_r_squared"] > 0.05:
                    warnings.append(
                        "Large gap between R² and adjusted R²: extra terms add little."
                    )
                if metrics["adjusted_r_squared"] - base["adjusted_r_squared"] < 0.02:
                    warnings.append(
                        f"{m['parameters'] - linear['parameters']} extra parameter(s) "
                        "improve adjusted R² by less than 0.02 over linear."
                    )
                if base["aic"] - metrics["aic"] < 2:
                    warnings.append(
                        "AIC is not at least 2 lower than linear, so the data does "
                        "not support the extra complexity."
                    )
                divergence = abs(m["forecast"][-1] - linear["forecast"][-1])
                if divergence > 2 * max(base["rmse"], 1e-9):
                    warnings.append(
                        f"Extrapolation diverges from the linear trend by "
                        f"{divergence:.2f} astronauts at +{PREDICTION_HORIZON_PERIODS} "
                        "periods; polynomial forecasts are unreliable outside the data."
                    )
            m["overfitting_warnings"] = warnings

        # ---------------- Best model and recommendations ----------------
        best = models[0]
        recommendations = []
        if best["degree"] == 1:
            recommendations.append(
                "Linear wins: the astronaut count follows a straight-line trend. Use "
                "the linear model (build_regression_model) for forecasting."
            )
        elif best["degree"] == 2:
            curvature = best["fit"]["coefficients"][2]
            recommendations.append(
                "Quadratic wins: the trend is "
                + ("accelerating" if curvature > 0 else "decelerating")
                + ". Use it for short horizons only and re-check as real runs accrue."
            )
        else:
            recommendations.append(
                "Cubic wins in-sample, but cubic curves swing sharply outside the "
                "data. Use it to describe the past; confirm with cross-validation "
                "before using it to forecast."
            )
        if best["overfitting_warnings"] and linear and best is not linear:
            recommendations.append(
                "The winner shows overfitting signs; prefer the linear model for "
                "forecasting until more real data is available."
            )
        if linear and best is not linear and linear["rank"] == 2:
            recommendations.append(
                f"Linear is runner-up ({linear['composite_score']} vs "
                f"{best['composite_score']}); the simpler model is a safe fallback."
            )
        if result["observed_periods"] < n / 2:
            recommendations.append(
                f"Only {result['observed_periods']} of {n} training periods are "
                "real observations; the ranking mostly reflects synthetic data."
            )

        for m in models:
            m.pop("fit")
        result.update(
            {
                "status": "compared",
                "composite_weights": COMPOSITE_WEIGHTS,
                "models": models,
                "best_model": best["model"],
                "best_model_degree": best["degree"],
                "recommendations": recommendations,
            }
        )

        # ---------------- Comparison table ----------------
        width = 96
        print("=" * width)
        print(f"REGRESSION MODEL COMPARISON ({n} periods)")
        print("=" * width)
        print(
            f"{'Rank':<5}{'Medal':<8}{'Model':<24}{'R2':>8}{'Adj R2':>9}"
            f"{'RMSE':>9}{'MAE':>9}{'AIC':>10}{'Score':>9}"
        )
        print("-" * width)
        for m in models:
            x = m["metrics"]
            print(
                f"{m['rank']:<5}{m['medal'] or '':<8}{m['model']:<24}"
                f"{x['r_squared']:>8.4f}{x['adjusted_r_squared']:>9.4f}"
                f"{x['rmse']:>9.4f}{x['mae']:>9.4f}{x['aic']:>10.2f}"
                f"{m['composite_score']:>9.2f}"
            )
        print("-" * width)
        print("EQUATIONS (t = period index)")
        for m in models:
            print(f"  {m['model']:<24} {m['equation']}")
        print("FORECASTS (next periods)")
        for m in models:
            print(f"  {m['model']:<24} {m['forecast']}")
        print("-" * width)
        print(
            f"WINNER: {best['model']} ({best['medal']}, score {best['composite_score']})"
        )
        for m in models:
            for w in m["overfitting_warnings"]:
                print(f"  WARNING [{m['model']}]: {w}")
        for r in recommendations:
            print(f"  - {r}")
        print("=" * width)
        return result

    @task
    def cross_validate_models(
        regression_model: dict, model_comparison: dict, **context
    ) -> dict:
        """
        This task performs k-fold cross-validation and backtesting on the
        regression models (Linear, Polynomial degree 2 and 3) to validate
        their predictive accuracy. It uses the simulated historical series
        from `build_regression_model` and measures forecast error with RMSE,
        MAE, MAPE, bias, max error, out-of-sample R squared and directional
        accuracy.

        - k-fold: the periods are shuffled (seeded by run date) into
          CV_FOLDS folds; each fold is predicted by a model trained on the
          others.
        - Backtesting: walk-forward from BACKTEST_MIN_TRAIN periods, training
          only on the past and forecasting 1 to PREDICTION_HORIZON_PERIODS
          periods ahead, as the model would be used in production.
        """
        series = regression_model.get("series", [])
        xs = [p["period"] for p in series]
        ys = [p["astronauts"] for p in series]
        n = len(series)
        if n < BACKTEST_MIN_TRAIN + 2:
            print(f"Only {n} periods; not enough for cross-validation.")
            return {"status": "insufficient_data", "periods": n, "models": []}

        rng = random.Random(f"cv-{context['ds']}")
        shuffled = list(range(n))
        rng.shuffle(shuffled)
        k = min(CV_FOLDS, n)
        folds = [shuffled[i::k] for i in range(k)]
        in_sample_rmse = {
            m["model"]: m["metrics"]["rmse"] for m in model_comparison.get("models", [])
        }

        results = []
        for name, degree in COMPARISON_MODELS:
            # ---------------- k-fold cross-validation ----------------
            fold_results, cv_actual, cv_pred = [], [], []
            for number, test_idx in enumerate(folds, start=1):
                test_set = set(test_idx)
                train = [i for i in range(n) if i not in test_set]
                fit = (
                    _fit_polynomial(
                        [xs[i] for i in train], [ys[i] for i in train], degree
                    )
                    if len(train) >= degree + 2
                    else None
                )
                if fit is None:
                    continue
                actual = [ys[i] for i in test_idx]
                predicted = [_predict_polynomial(fit, xs[i]) for i in test_idx]
                fold_results.append(
                    {
                        "fold": number,
                        "train_size": len(train),
                        **_error_metrics(actual, predicted),
                    }
                )
                cv_actual += actual
                cv_pred += predicted
            fold_rmses = [f["rmse"] for f in fold_results]
            kfold = {
                "folds": len(fold_results),
                **_error_metrics(cv_actual, cv_pred),
                "rmse_std_across_folds": round(statistics.pstdev(fold_rmses), 4)
                if len(fold_rmses) > 1
                else None,
                "fold_results": fold_results,
            }

            # ---------------- Walk-forward backtesting ----------------
            by_horizon = {h: ([], []) for h in range(1, PREDICTION_HORIZON_PERIODS + 1)}
            hits = moves = origins = 0
            for origin in range(max(BACKTEST_MIN_TRAIN, degree + 2), n):
                fit = _fit_polynomial(xs[:origin], ys[:origin], degree)
                if fit is None:
                    continue
                origins += 1
                for h in by_horizon:
                    target = origin - 1 + h
                    if target >= n:
                        break
                    by_horizon[h][0].append(ys[target])
                    by_horizon[h][1].append(_predict_polynomial(fit, xs[target]))
                actual_move = ys[origin] - ys[origin - 1]
                predicted_move = _predict_polynomial(fit, xs[origin]) - ys[origin - 1]
                if actual_move != 0:
                    moves += 1
                    hits += (actual_move > 0) == (predicted_move > 0)
            all_actual = [a for acts, _ in by_horizon.values() for a in acts]
            all_pred = [p for _, preds in by_horizon.values() for p in preds]
            backtest = {
                "origins": origins,
                **_error_metrics(all_actual, all_pred),
                "directional_accuracy_pct": round(hits / moves * 100, 2)
                if moves
                else None,
                "by_horizon": {
                    f"+{h}": _error_metrics(acts, preds)
                    for h, (acts, preds) in by_horizon.items()
                    if acts
                },
            }

            fit_rmse = in_sample_rmse.get(name)
            gap = (
                round(kfold["rmse"] / fit_rmse, 3)
                if fit_rmse and kfold.get("rmse") is not None
                else None
            )
            results.append(
                {
                    "model": name,
                    "degree": degree,
                    "kfold": kfold,
                    "backtest": backtest,
                    "in_sample_rmse": fit_rmse,
                    "generalization_ratio": gap,
                    "overfitting_flag": gap is not None and gap > 1.5,
                }
            )

        # ---------------- Verdict ----------------
        scored = [r for r in results if r["backtest"].get("rmse") is not None]
        best_cv = min(results, key=lambda r: r["kfold"].get("rmse", math.inf))["model"]
        best_backtest = (
            min(scored, key=lambda r: r["backtest"]["rmse"]) if scored else None
        )
        comparison_winner = model_comparison.get("best_model")
        if best_backtest and comparison_winner == best_cv == best_backtest["model"]:
            verdict = "validated"
        elif best_backtest and comparison_winner in (best_cv, best_backtest["model"]):
            verdict = "partially_validated"
        else:
            verdict = "not_validated"
        mape = best_backtest["backtest"].get("mape_pct") if best_backtest else None
        accuracy_rating = (
            "Unknown"
            if mape is None
            else "Excellent"
            if mape < 5
            else "Good"
            if mape < 10
            else "Moderate"
            if mape < 20
            else "Poor"
        )
        summary = {
            "comparison_winner": comparison_winner,
            "best_by_kfold_rmse": best_cv,
            "best_by_backtest_rmse": best_backtest["model"] if best_backtest else None,
            "recommended_for_forecasting": best_backtest["model"]
            if best_backtest
            else best_cv,
            "verdict": verdict,
            "forecast_accuracy_rating": accuracy_rating,
            "note": "Backtesting is the better guide for forecasting: k-fold "
            "shuffles time, so models see the future of the periods they predict.",
        }
        output = {
            "status": "validated",
            "periods": n,
            "folds": k,
            "backtest_min_train": BACKTEST_MIN_TRAIN,
            "models": results,
            "summary": summary,
        }

        width = 96
        print("=" * width)
        print(f"CROSS-VALIDATION ({k}-fold) AND WALK-FORWARD BACKTEST ({n} periods)")
        print("=" * width)
        print(
            f"{'Model':<24}{'CV RMSE':>9}{'CV MAE':>9}{'CV MAPE%':>10}"
            f"{'BT RMSE':>9}{'BT MAE':>9}{'BT MAPE%':>10}{'Dir%':>8}{'Gen':>7}"
        )
        print("-" * width)
        for r in results:
            cv, bt = r["kfold"], r["backtest"]

            def fmt(value, spec):
                return format(value, spec) if value is not None else "n/a"

            print(
                f"{r['model']:<24}{fmt(cv.get('rmse'), '>9.4f'):>9}"
                f"{fmt(cv.get('mae'), '>9.4f'):>9}{fmt(cv.get('mape_pct'), '>10.2f'):>10}"
                f"{fmt(bt.get('rmse'), '>9.4f'):>9}{fmt(bt.get('mae'), '>9.4f'):>9}"
                f"{fmt(bt.get('mape_pct'), '>10.2f'):>10}"
                f"{fmt(bt.get('directional_accuracy_pct'), '>8.1f'):>8}"
                f"{fmt(r['generalization_ratio'], '>7.2f'):>7}"
            )
        print("-" * width)
        print("BACKTEST RMSE BY HORIZON")
        for r in results:
            horizons = ", ".join(
                f"{h}: {m['rmse']}" for h, m in r["backtest"]["by_horizon"].items()
            )
            print(f"  {r['model']:<24} {horizons}")
            if r["overfitting_flag"]:
                print(
                    f"  WARNING [{r['model']}]: CV error is {r['generalization_ratio']}x "
                    "the in-sample error; the model overfits."
                )
        print("-" * width)
        for key, value in summary.items():
            print(f"  {key}: {value}")
        print("=" * width)
        return output

    @task
    def export_analysis_report(
        astronaut_stats: dict,
        statistical_analysis: dict,
        trends: dict,
        correlation: dict,
        data_quality: dict,
        comprehensive_analysis: dict,
        summary_report: dict,
        insights_report: dict,
        dashboard: dict,
        regression_model: dict,
        model_comparison: dict,
        cross_validation: dict,
        **context,
    ) -> dict:
        """
        This task exports all analysis results to JSON and CSV formats for
        downstream consumption. It creates structured files under
        EXPORT_BASE_DIR/<date>/<run_id>/ that can be picked up by external
        systems, data warehouses, or visualization tools:

        - analysis_report.json: every analysis output in one document
        - one CSV per table (crew, data quality, KPIs, predictions, models,
          cross-validation, feature importance, trend history, alerts)
        - manifest.json: the list of files with row counts and sizes

        On Astro, /tmp is local to the worker and is not kept after the task
        ends, so change EXPORT_BASE_DIR to shared or object storage for
        production use.
        """
        safe_run_id = "".join(
            ch if ch.isalnum() or ch in "-_." else "_" for ch in context["run_id"]
        )
        out_dir = Path(EXPORT_BASE_DIR) / context["ds"] / safe_run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        files = []

        def write_json(name: str, payload: dict) -> None:
            path = out_dir / name
            path.write_text(json.dumps(payload, indent=2, default=str))
            files.append(
                {"file": str(path), "format": "json", "bytes": path.stat().st_size}
            )

        def write_csv(name: str, rows: list[dict]) -> None:
            path = out_dir / name
            fieldnames = list(dict.fromkeys(key for row in rows for key in row))
            with path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames or ["empty"])
                writer.writeheader()
                writer.writerows(rows)
            files.append(
                {
                    "file": str(path),
                    "format": "csv",
                    "rows": len(rows),
                    "bytes": path.stat().st_size,
                }
            )

        metadata = {
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "logical_date": context["ts"],
            "exported_at": pendulum.now("UTC").to_iso8601_string(),
        }

        # ---------------- Full JSON report ----------------
        write_json(
            "analysis_report.json",
            {
                "metadata": metadata,
                "astronaut_stats": astronaut_stats,
                "statistical_analysis": statistical_analysis,
                "trends": trends,
                "correlation": correlation,
                "data_quality": data_quality,
                "comprehensive_analysis": comprehensive_analysis,
                "summary_report": summary_report,
                "insights_report": insights_report,
                "dashboard": dashboard,
                "regression_model": regression_model,
                "model_comparison": model_comparison,
                "cross_validation": cross_validation,
            },
        )

        # ---------------- CSV tables ----------------
        capacity = astronaut_stats.get("capacity_metrics", {})
        write_csv(
            "crew_distribution.csv",
            [
                {
                    "craft": craft,
                    "crew": d.get("count"),
                    "share_pct": d.get("share_pct"),
                    "nominal_capacity": capacity.get(craft, {}).get("nominal_capacity"),
                    "utilization_pct": capacity.get(craft, {}).get("utilization_pct"),
                }
                for craft, d in astronaut_stats.get("distribution", {}).items()
            ],
        )
        write_csv("data_quality_checks.csv", data_quality.get("checks", []))
        write_csv(
            "dashboard_kpis.csv",
            [
                {
                    "kpi": k["kpi"].strip(),
                    "value": k["value"],
                    "unit": k["unit"],
                    "status": k["status"],
                    "trend": (k.get("trend") or {}).get("direction"),
                }
                for k in dashboard.get("kpis", [])
            ],
        )
        write_csv(
            "regression_predictions.csv",
            [
                {
                    "period_ahead": p["period_ahead"],
                    "predicted_astronauts": p["predicted_astronauts"],
                    "trend_prediction": p["trend_prediction"],
                    "weather_adjustment": p["weather_adjustment"],
                    "momentum_adjustment": p["momentum_adjustment"],
                    "ci95_low": p["confidence_interval_95"]["low"],
                    "ci95_high": p["confidence_interval_95"]["high"],
                    "pi95_low": p["prediction_interval_95"]["low"],
                    "pi95_high": p["prediction_interval_95"]["high"],
                }
                for p in regression_model.get("predictions", [])
            ],
        )
        write_csv(
            "feature_importance.csv", regression_model.get("feature_importance", [])
        )
        write_csv(
            "model_comparison.csv",
            [
                {
                    "rank": m["rank"],
                    "medal": m["medal"],
                    "model": m["model"],
                    **m["metrics"],
                    "composite_score": m["composite_score"],
                    "equation": m["equation"],
                    "overfitting_warnings": " | ".join(m["overfitting_warnings"]),
                }
                for m in model_comparison.get("models", [])
            ],
        )
        write_csv(
            "cross_validation.csv",
            [
                {
                    "model": r["model"],
                    "method": method,
                    **{
                        key: r[method].get(key)
                        for key in (
                            "n",
                            "rmse",
                            "mae",
                            "mape_pct",
                            "bias",
                            "max_abs_error",
                            "r_squared",
                        )
                    },
                    "directional_accuracy_pct": r[method].get(
                        "directional_accuracy_pct"
                    ),
                    "generalization_ratio": r["generalization_ratio"],
                }
                for r in cross_validation.get("models", [])
                for method in ("kfold", "backtest")
            ],
        )
        write_csv(
            "trend_history.csv",
            [
                {key: value for key, value in h.items() if key != "crew_by_craft"}
                for h in trends.get("history", [])
            ],
        )
        write_csv("alerts.csv", insights_report.get("critical_alerts", []))

        manifest = {**metadata, "directory": str(out_dir), "files": list(files)}
        write_json("manifest.json", manifest)
        manifest["files"] = files

        print(f"Exported {len(files)} file(s) to {out_dir}:")
        for f in files:
            rows = f" ({f['rows']} rows)" if "rows" in f else ""
            print(f"  - {Path(f['file']).name}{rows}, {f['bytes']} bytes")
        return manifest

    @task
    def generate_performance_dashboard(
        astronaut_stats: dict,
        trends: dict,
        data_quality: dict,
        comprehensive_analysis: dict,
        insights_report: dict,
        weather: dict,
        iss_location: dict,
        regression_model: dict,
    ) -> dict:
        """
        This task generates a performance dashboard with visual KPIs, trend
        indicators, and real-time metrics display. It provides an
        at-a-glance operational status with color-coded indicators
        (GREEN / AMBER / RED, GREY when there is no data) and simplified
        metrics for monitoring. The dashboard is printed to the task log and
        returned via XCom.
        """
        now = pendulum.now("UTC")
        trend_metrics = trends.get("metrics", {})
        efficiency = comprehensive_analysis.get("efficiency_metrics", {})

        def bar(pct, width: int = 20) -> str:
            """Text progress bar, capped at 100%."""
            if pct is None:
                return "[" + " " * width + "]"
            filled = round(min(max(pct, 0), 100) / 100 * width)
            return "[" + "#" * filled + "-" * (width - filled) + "]"

        def trend_indicator(metric: str) -> dict:
            t = trend_metrics.get(metric, {})
            direction = t.get("direction", "insufficient_history")
            arrow = {"increasing": "↑", "decreasing": "↓", "stable": "→"}.get(
                direction, "·"
            )
            return {
                "arrow": arrow,
                "direction": direction,
                "change": t.get("absolute_change"),
                "forecast_next": t.get("forecast_next"),
            }

        def utilization_status(pct) -> str:
            if pct is None:
                return "GREY"
            amber, red = UTILIZATION_THRESHOLDS
            return "RED" if pct > red else "AMBER" if pct >= amber else "GREEN"

        # ---------------- KPIs ----------------
        kpis: list[dict] = []

        def kpi(name: str, value, unit: str, status: str, trend=None, pct=None):
            kpis.append(
                {
                    "kpi": name,
                    "value": value,
                    "unit": unit,
                    "status": status,
                    "trend": trend,
                    "bar": bar(pct) if pct is not None else None,
                }
            )

        total = astronaut_stats.get("total_astronauts", 0)
        kpi(
            "Astronauts in space",
            total,
            "people",
            "GREEN" if total > 0 else "RED",
            trend_indicator("total_astronauts"),
        )
        kpi(
            "Active spacecraft",
            len(astronaut_stats.get("distribution", {})),
            "craft",
            "GREEN" if astronaut_stats.get("distribution") else "RED",
            trend_indicator("spacecraft_count"),
        )
        utilization = efficiency.get("overall_utilization_pct")
        kpi(
            "Capacity utilization",
            utilization,
            "%",
            utilization_status(utilization),
            pct=utilization,
        )
        for craft, pct in sorted(
            efficiency.get("utilization_by_craft_pct", {}).items()
        ):
            kpi(
                f"  {craft} utilization",
                pct,
                "%",
                utilization_status(pct),
                trend_indicator(f"crew:{craft}"),
                pct=pct,
            )
        dq_rate = efficiency.get("data_quality_pass_rate_pct")
        green_dq, amber_dq = DATA_QUALITY_THRESHOLDS
        kpi(
            "Data quality pass rate",
            dq_rate,
            "%",
            "GREY"
            if dq_rate is None
            else "GREEN"
            if dq_rate >= green_dq
            else "AMBER"
            if dq_rate >= amber_dq
            else "RED",
            pct=dq_rate,
        )
        overall_risk = comprehensive_analysis.get("risk_assessment", {}).get(
            "overall_risk"
        )
        kpi(
            "Overall risk",
            overall_risk,
            "level",
            {"low": "GREEN", "medium": "AMBER", "high": "RED"}.get(
                overall_risk, "GREY"
            ),
        )
        alerts = insights_report.get("critical_alerts", [])
        critical = sum(1 for a in alerts if a["severity"] == "critical")
        warnings = len(alerts) - critical
        kpi(
            "Open alerts",
            f"{critical} critical / {warnings} warning",
            "alerts",
            "RED" if critical else "AMBER" if warnings else "GREEN",
        )
        anomalies = len(comprehensive_analysis.get("anomalies", []))
        kpi("Anomalies detected", anomalies, "count", "AMBER" if anomalies else "GREEN")
        predictions = regression_model.get("predictions", [])
        if predictions:
            nxt = predictions[0]
            kpi(
                "Predicted astronauts (+1)",
                f"{nxt['predicted_astronauts_rounded']} "
                f"({nxt['interval_low']}-{nxt['interval_high']})",
                "people",
                "GREEN" if nxt["predicted_astronauts_rounded"] > 0 else "RED",
            )
        else:
            kpi("Predicted astronauts (+1)", "n/a (building history)", "people", "GREY")
        decisions = len(insights_report.get("decision_support", []))
        kpi(
            "Decisions pending",
            decisions,
            "count",
            "GREEN" if decisions <= 1 else "AMBER",
        )

        # ---------------- Real-time metrics ----------------
        iss_ts = iss_location.get("timestamp")
        iss_age = (
            (now - pendulum.from_timestamp(iss_ts, tz="UTC")).in_minutes()
            if isinstance(iss_ts, (int, float))
            else None
        )
        wind = weather.get("wind_speed_kmh")
        amber_wind, red_wind = WIND_THRESHOLDS_KMH
        stale = [
            c["source"]
            for c in data_quality.get("checks", [])
            if c["dimension"] == "timeliness" and c["status"] == "FAIL"
        ]
        realtime = {
            "iss_position": {
                "latitude": iss_location.get("latitude"),
                "longitude": iss_location.get("longitude"),
                "age_minutes": iss_age,
                "status": "GREY"
                if iss_age is None
                else "GREEN"
                if iss_age <= ISS_MAX_AGE_MINUTES
                else "AMBER",
            },
            "jsc_weather": {
                "temperature_c": weather.get("temperature_c"),
                "wind_speed_kmh": wind,
                "cloud_cover_pct": weather.get("cloud_cover_pct"),
                "observed_at": weather.get("observed_at"),
                "status": "GREY"
                if wind is None
                else "RED"
                if wind >= red_wind
                else "AMBER"
                if wind >= amber_wind
                else "GREEN",
            },
            "data_freshness": {
                "stale_sources": stale,
                "status": "AMBER" if stale else "GREEN",
            },
        }

        # ---------------- Overall status ----------------
        statuses = [k["status"] for k in kpis] + [
            m["status"] for m in realtime.values()
        ]
        overall_status = max(statuses, key=lambda s: STATUS_RANK[s])
        status_counts = {
            s: statuses.count(s) for s in ("GREEN", "AMBER", "RED", "GREY")
        }

        dashboard = {
            "generated_at": now.to_iso8601_string(),
            "overall_status": overall_status,
            "status_counts": status_counts,
            "headline": insights_report.get("headline"),
            "kpis": kpis,
            "realtime": realtime,
            "top_alerts": alerts[:3],
        }

        width = 72
        print("=" * width)
        print(f"PERFORMANCE DASHBOARD - {dashboard['generated_at']}")
        print(
            f"OVERALL STATUS: {overall_status}   "
            + "  ".join(f"{s}={n}" for s, n in status_counts.items() if n)
        )
        print("=" * width)
        if dashboard["headline"]:
            print(dashboard["headline"])
            print("-" * width)
        print("KPIs")
        for k in kpis:
            trend = k["trend"] or {}
            trend_text = (
                f"{trend['arrow']} {trend['change']:+}"
                if trend.get("change") is not None
                else trend.get("arrow", "")
            )
            value = f"{k['value']}{k['unit'] if k['unit'] == '%' else ''}"
            print(
                f"  [{k['status']:<5}] {k['kpi']:<26} {value:<22} "
                f"{k['bar'] or '':<22} {trend_text}"
            )
        print("-" * width)
        print("REAL-TIME")
        iss = realtime["iss_position"]
        print(
            f"  [{iss['status']:<5}] ISS position              "
            f"lat {iss['latitude']}, lon {iss['longitude']} ({iss['age_minutes']} min old)"
        )
        wx = realtime["jsc_weather"]
        print(
            f"  [{wx['status']:<5}] JSC weather               "
            f"{wx['temperature_c']}°C, wind {wx['wind_speed_kmh']} km/h, "
            f"cloud {wx['cloud_cover_pct']}%"
        )
        fresh = realtime["data_freshness"]
        print(
            f"  [{fresh['status']:<5}] Data freshness            "
            + (
                f"stale: {', '.join(fresh['stale_sources'])}"
                if fresh["stale_sources"]
                else "all sources fresh"
            )
        )
        if alerts:
            print("-" * width)
            print("TOP ALERTS")
            for a in alerts[:3]:
                print(f"  [{a['severity'].upper()}] {a['title']}: {a['details']}")
        print("=" * width)
        return dashboard

    astronauts = get_astronauts()

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronauts  # Define dependencies using TaskFlow API syntax
    )

    spacecraft_summary = summarise_spacecraft(astronauts)

    weather = get_weather_data()

    astronaut_stats = calculate_astronauts_stats(astronauts)

    correlation = analyze_correlation(weather=weather, astronaut_stats=astronaut_stats)

    summary_report = generate_summary_report(
        people_in_space=astronauts,
        weather=weather,
        correlation=correlation,
        astronaut_stats=astronaut_stats,
    )

    iss_location = get_iss_location()

    data_quality = validate_data_quality(
        people_in_space=astronauts,
        weather=weather,
        iss_location=iss_location,
        spacecraft_summary=spacecraft_summary,
        astronaut_stats=astronaut_stats,
        correlation=correlation,
    )

    statistical_analysis = perform_statistical_methods(
        people_in_space=astronauts, astronaut_stats=astronaut_stats
    )

    trends = calculate_trends(astronaut_stats=astronaut_stats, weather=weather)

    regression_model = build_regression_model(
        astronaut_stats=astronaut_stats, trends=trends, weather=weather
    )

    comprehensive_analysis = perform_comprehensive_analysis(
        astronaut_stats=astronaut_stats,
        statistical_analysis=statistical_analysis,
        trends=trends,
        correlation=correlation,
        data_quality=data_quality,
        weather=weather,
    )

    insights_report = generate_insights_report(
        astronaut_stats=astronaut_stats,
        statistical_analysis=statistical_analysis,
        trends=trends,
        correlation=correlation,
        data_quality=data_quality,
        comprehensive_analysis=comprehensive_analysis,
        summary_report=summary_report,
    )

    model_comparison = compare_regression_models(
        people_in_space=astronauts,
        astronaut_stats=astronaut_stats,
        trends=trends,
        weather=weather,
        regression_model=regression_model,
    )

    cross_validation = cross_validate_models(
        regression_model=regression_model, model_comparison=model_comparison
    )

    dashboard = generate_performance_dashboard(
        astronaut_stats=astronaut_stats,
        trends=trends,
        data_quality=data_quality,
        comprehensive_analysis=comprehensive_analysis,
        insights_report=insights_report,
        weather=weather,
        iss_location=iss_location,
        regression_model=regression_model,
    )

    export_analysis_report(
        astronaut_stats=astronaut_stats,
        statistical_analysis=statistical_analysis,
        trends=trends,
        correlation=correlation,
        data_quality=data_quality,
        comprehensive_analysis=comprehensive_analysis,
        summary_report=summary_report,
        insights_report=insights_report,
        dashboard=dashboard,
        regression_model=regression_model,
        model_comparison=model_comparison,
        cross_validation=cross_validation,
    )


# Instantiate the DAG
example_astronauts()


@dag(
    start_date=datetime(2024, 1, 1),
    # Data-aware scheduling: this DAG runs once BOTH datasets have been
    # updated since its last run. Use `|` instead of `&` to run when
    # either one is updated.
    schedule=(current_astronauts_dataset & iss_location_dataset),
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_iss_dataset_consumer():
    @task
    def report_dataset_updates(**context) -> None:
        """
        This task prints which Dataset events triggered this DAG run,
        including the upstream DAG and task that produced each update.
        """
        triggering_events = context["triggering_dataset_events"]
        for dataset_uri, events in triggering_events.items():
            for event in events:
                print(
                    f"Dataset '{dataset_uri}' was updated by "
                    f"{event.source_dag_id}.{event.source_task_id} "
                    f"at {event.timestamp}"
                )

    report_dataset_updates()


example_iss_dataset_consumer()


@dag(
    start_date=datetime(2024, 1, 1),
    # Runs every time the `spacecraft_summary` Dataset is updated
    schedule=[spacecraft_summary_dataset],
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_craft_summary_consumer():
    @task
    def report_craft_summary(**context) -> None:
        """
        This task reads the `extra` metadata attached to the Dataset event
        that triggered this run and prints the per-craft headcount.
        """
        triggering_events = context["triggering_dataset_events"]
        for event in triggering_events[spacecraft_summary_dataset.uri]:
            extra = event.extra or {}
            print(
                f"{extra.get('total_astronauts', 'Unknown')} people in space "
                f"(reported by {event.source_dag_id} at {event.timestamp}):"
            )
            for craft, count in extra.get("spacecraft_counts", {}).items():
                print(f"  - {craft}: {count}")

    report_craft_summary()


example_craft_summary_consumer()
