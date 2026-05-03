# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from os import path

__version__ = open(path.join(path.dirname(__file__), '_version.txt')).read().strip()


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-box",
        "name": "Box",
        "description": "`Box <https://www.box.com/>`__\n",
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "Box",
                "external-doc-url": "https://developer.box.com/",
                "logo": "/docs/integration-logos/Box.png",
                "tags": ["service"],
            }
        ],
        "operators": [
            {
                "integration-name": "Box",
                "python-modules": ["airflow_providers_box.operators.box"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Box",
                "python-modules": ["airflow_providers_box.hooks.box"],
            }
        ],
        "sensors": [
            {
                "integration-name": "Box",
                "python-modules": ["airflow_providers_box.sensors.box"],
            }
        ],
        "triggers": [
            {
                "integration-name": "Box",
                "python-modules": ["airflow_providers_box.triggers.box"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow_providers_box.hooks.box.BoxHook",
                "connection-type": "box",
                "hook-name": "Box",
                "conn-fields": {
                    "client_id": {"label": "Client ID", "schema": {"type": "string"}},
                    "client_secret": {
                        "label": "Client Secret",
                        "schema": {"type": "string", "format": "password"},
                    },
                    "enterprise_id": {
                        "label": "Enterprise ID",
                        "schema": {"type": "string", "format": "password"},
                    },
                },
                "ui-field-behaviour": {
                    "hidden-fields": ["port", "password", "login", "schema", "host"],
                    "relabeling": {},
                    "placeholders": {"client_id": "", "client_secret": "", "enterprise_id": ""},
                },
            }
        ],
        "asset-uris": [{"schemes": ["box"]}],
    }
