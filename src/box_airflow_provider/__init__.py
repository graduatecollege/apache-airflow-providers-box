__version__ = "1.0.0"


## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "box-airflow-provider",
        "name": "Box",
        "description": "Apache Airflow provider for connecting to Box.com storage.",
        "connection-types": [
            {
                "connection-type": "box",
                "hook-class-name": "box_airflow_provider.hooks.box.BoxHook"
            }
        ],
        "versions": [__version__],
    }
