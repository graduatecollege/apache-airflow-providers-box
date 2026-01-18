# box-airflow-provider

Provider for integrating Box.com with Apache Airflow. Written for Apache Airflow 2.11.0.

See the [sample basic.py DAG](dags/basic.py) for usage examples.

## Support

This product is supported by the Graduate College, University of Illinois Urbana-Champaign on a best-effort basis.

As of the last update to this README, the expected End-of-Life and End-of-Support dates of this product are based on these dependencies:

- Box SDK Gen (v10+): Active development
- Apache Airflow 2.11: 2026-04-22
- Python 3.12: 2028-08-31

## Migration from Box SDK v3

This provider has been migrated from Box SDK v3 (boxsdk) to Box SDK v10+ (box-sdk-gen). The new SDK is auto-generated from Box's OpenAPI specification and provides:
- Comprehensive API coverage
- Strong typing with type hints
- Explicit data models
- Immutable design patterns
- Rapid feature availability