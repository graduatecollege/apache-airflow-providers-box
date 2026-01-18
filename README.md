# box-airflow-provider

Provider for integrating Box.com with Apache Airflow. Written for Apache Airflow 2.11.0.

## Installation

Install from PyPI:

```bash
pip install box-airflow-provider
```

## Usage

See the [sample basic.py DAG](dags/basic.py) for usage examples.

## Support

This product is supported by the Graduate College, University of Illinois Urbana-Champaign on a best-effort basis.

As of the last update to this README, the expected End-of-Life and End-of-Support dates 
of this product are 2026-04-22.

End-of-Life was decided upon based on these dependencies:

- Box SDK Gen (v10+): [Box SDK v10 EOL is TBD](https://github.com/box/box-python-sdk?tab=readme-ov-file#versioning)
- Apache Airflow 2.11: 2026-04-22
- Python 3.12: 2028-08-31

## Release Process

This project is automatically published to PyPI and GitHub Releases when a tag with the format `v*` (e.g., `v1.0.0`) is pushed to the main branch.

To create a new release:

1. Update the version in `src/box_airflow_provider/__init__.py`
2. Commit the version change
3. Create and push a tag:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```
4. The GitHub Actions workflow will automatically:
   - Build the package
   - Publish to PyPI
   - Create a GitHub Release with the distribution files

**Note**: Ensure that the `PYPI_API_TOKEN` secret is configured in the repository settings for PyPI publishing to work. The workflow uses trusted publishing (OIDC) by default, but you may need to configure your PyPI project to allow this.
