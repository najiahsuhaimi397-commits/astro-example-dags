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
import pendulum
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

# Datasets shared between the producer DAG and the consumer DAG in this file
current_astronauts_dataset = Dataset("current_astronauts")
iss_location_dataset = Dataset("iss_location")
# A third dataset example: updated by `summarise_spacecraft`, its events carry
# extra metadata (a per-spacecraft headcount) that downstream DAGs can read.
spacecraft_summary_dataset = Dataset("spacecraft_summary")


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

    generate_summary_report(
        people_in_space=astronauts,
        weather=weather,
        correlation=correlation,
        astronaut_stats=astronaut_stats,
    )

    iss_location = get_iss_location()

    validate_data_quality(
        people_in_space=astronauts,
        weather=weather,
        iss_location=iss_location,
        spacecraft_summary=spacecraft_summary,
        astronaut_stats=astronaut_stats,
        correlation=correlation,
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
