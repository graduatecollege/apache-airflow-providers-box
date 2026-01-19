# read version from version.txt
from os import path

__version__ = open(path.join(path.dirname(__file__), '_version.txt')).read().strip()

## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-box",
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
