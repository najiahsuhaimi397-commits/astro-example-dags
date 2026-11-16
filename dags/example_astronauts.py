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

# Datasets shared between the producer DAG and the consumer DAG in this file
current_astronauts_dataset = Dataset("current_astronauts")
iss_location_dataset = Dataset("iss_location")


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

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=get_astronauts()  # Define dependencies using TaskFlow API syntax
    )

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
