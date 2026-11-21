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

    astronauts = get_astronauts()

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronauts  # Define dependencies using TaskFlow API syntax
    )

    summarise_spacecraft(astronauts)

    weather = get_weather_data()

    astronaut_stats = calculate_astronauts_stats(astronauts)

    analyze_correlation(weather=weather, astronaut_stats=astronaut_stats)

    get_iss_location()


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
