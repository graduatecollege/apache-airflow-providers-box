__version__ = "1.0.0"



## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "airflow-provider-box",  # Required
        "name": "Box",  # Required
        "description": "A sample template for Apache Airflow providers.",  # Required
        "connection-types": [
            {
                "connection-type": "box",
                "hook-class-name": "box.hooks.box.BoxHook"
            }
        ],
        "versions": [__version__],  # Required
    }
