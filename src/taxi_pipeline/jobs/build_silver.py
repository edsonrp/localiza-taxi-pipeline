"""Build the treated/silver layer.

Next implementation step:
- read bronze green taxi trips
- apply data quality rules
- segregate valid and invalid records with error_type
- deduplicate when applicable
- write valid records to data/silver/green_taxi_trips_valid
- write invalid records to data/silver/green_taxi_trips_invalid
"""


def main() -> None:
    raise NotImplementedError("Next step: implement silver data quality rules.")


if __name__ == "__main__":
    main()
