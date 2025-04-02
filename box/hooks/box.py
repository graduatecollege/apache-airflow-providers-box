from __future__ import annotations

from typing import Any, Tuple, Literal, NamedTuple, Optional, Generic, TypeVar, Dict, TypedDict
import os
from dataclasses import dataclass

from boxsdk import Client, CCGAuth
from requests.auth import HTTPBasicAuth

from airflow.hooks.base import BaseHook


@dataclass
class BoxFileInfo:
    """Information about a Box file."""
    name: str
    type: str
    size: int
    created_at: str
    modified_at: str
    path: str
    updated: bool

class BoxResult[T]:
    """
    Generic result object for Box operations.
    
    :param id: The Box item ID if available
    :param success: Whether the operation was successful
    :param result: The operation-specific result data
    :param error_message: Error message if any error occurred during operation
    """

    def __init__(
            self,
            id: str | None = None,
            success: bool | None = None,
            result: T | None = None,
            error_message: str | None = None
    ):
        self.id = id
        self.success = success
        self.result = result
        self.error_message = error_message

        if success is None:
            if error_message is not None:
                self.success = False
            elif result is not None or id is not None:
                self.success = True
            else:
                self.success = False

    def __bool__(self) -> bool:
        return self.success


class BoxHook(BaseHook):
    """
    Interact with Box API.

    :param box_conn_id: The connection ID to use for Box API
    """
    conn_name_attr = "box_conn_id"
    default_conn_name = "box_default"
    conn_type = "box"
    hook_name = "Box"

    def __init__(self, box_conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__()
        self.client: Client | None = None

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "client_id": StringField(lazy_gettext("Client ID"), widget=BS3TextFieldWidget()),
            "client_secret": PasswordField(lazy_gettext("Client Secret"), widget=BS3PasswordFieldWidget()),
            "enterprise_id": PasswordField(lazy_gettext("Enterprise ID"), widget=BS3PasswordFieldWidget()),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour"""
        import json

        return {
            "hidden_fields": ["port", "password", "login", "schema", "host"],
            "relabeling": {},
            "placeholders": {
                "client_id": "",
                "client_secret": "",
                "enterprise_id": ""
            },
        }

    def __init__(
            self,
            box_conn_id: str = default_conn_name
    ) -> None:
        super().__init__()
        self.box_conn_id = box_conn_id

    def get_conn(self) -> Client:
        if self.box_conn_id and not self.client:
            conn = self.get_connection(self.box_conn_id)

            client_id = conn.client_id
            client_secret = conn.client_secret
            enterprise_id = conn.enterprise_id

            if not client_id or not client_secret or not enterprise_id:
                raise ValueError("Client ID, Client Secret and Enterprise ID must be provided in connection")

            auth = CCGAuth(
                client_id=client_id,
                client_secret=client_secret,
                enterprise_id=enterprise_id,
            )

            self.client = Client(auth)

        return self.client

    def test_connection(self) -> (bool, str):
        """Test a connection"""
        try:
            conn = self.get_conn()
            user = conn.user().get()

            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)

    def get_item_id(self, path: str, item_type: Literal['file', 'folder']) -> BoxResult[None]:
        """
        Get the ID of a Box item (file or folder) based on a path.

        :param path: Path to the item (e.g. "/Test/Area" or "/Test/Area/document.pdf")
        :param item_type: Type of item to look for, either 'file' or 'folder'
        :return: BoxResult containing the ID of the target item if found
        """
        try:
            client = self.get_conn()

            current_folder_id = '0'

            clean_path = path.strip('/')
            if not clean_path:
                if item_type == 'folder':
                    return BoxResult(id=current_folder_id)
                else:
                    return BoxResult(
                        error_message="Cannot get file ID for root folder path"
                    )

            path_parts = clean_path.split('/')

            # If looking for a file, the last part is the filename
            if item_type == 'file':
                folder_names = path_parts[:-1]
                file_name = path_parts[-1]
            else:
                folder_names = path_parts
                file_name = None

            for folder_name in folder_names:
                if not folder_name:
                    continue

                items = client.folder(folder_id=current_folder_id).get_items()

                found = False
                for item in items:
                    if item.type == 'folder' and item.name == folder_name:
                        current_folder_id = item.id
                        found = True
                        break

                if not found:
                    return BoxResult(
                        error_message=f"Folder '{folder_name}' not found in path '{path}'"
                    )

            # If looking for a file, find the file in the final folder
            if item_type == 'file' and file_name:
                items = client.folder(folder_id=current_folder_id).get_items()

                for item in items:
                    if item.type == 'file' and item.name == file_name:
                        return BoxResult(id=item.id)

                return BoxResult(
                    error_message=f"File '{file_name}' not found in folder"
                )

            return BoxResult(id=current_folder_id)
        except Exception as e:
            return BoxResult(error_message=str(e))

    def get_folder_id(self, path: str) -> BoxResult[None]:
        """
        Get the ID of a Box folder based on a path.

        :param path: Path to the folder (e.g. "/Test/Area")
        :return: BoxResult containing the ID of the target folder if found
        """
        return self.get_item_id(path, 'folder')

    def get_file_id(self, path: str) -> BoxResult[None]:
        """
        Get the ID of a Box file based on a path.

        :param path: Path to the file (e.g. "/Test/Area/document.pdf")
        :return: BoxResult containing the ID of the target file if found
        """
        return self.get_item_id(path, 'file')

    def get_file_modified_time(self, path: str) -> BoxResult[str]:
        """
        Get the last modified time of a Box file based on a path.
        
        :param path: Path to the file (e.g. "/Test/Area/document.pdf")
        :return: BoxResult with the file's modified time as the result if successful
        """
        try:
            # First, get the file ID using the existing function
            file_result = self.get_file_id(path)

            if not file_result.success or file_result.id is None:
                return BoxResult(
                    error_message=file_result.error_message or "File not found"
                )

            # Get the file info
            client = self.get_conn()
            file_info = client.file(file_id=file_result.id).get()

            # Return the result with file ID and its modified time
            return BoxResult(
                id=file_info.id,
                result=file_info.modified_at
            )

        except Exception as e:
            return BoxResult(error_message=str(e))

    def upload_file(self, local_file_path: str, box_path: str) -> BoxResult[BoxFileInfo]:
        """
        Upload a file to Box.

        :param local_file_path: Path to the local file to upload
        :param box_path: Destination path in Box (e.g. "/Test/Area/document.pdf")
        :return: BoxResult with the uploaded file information
        """
        try:
            client = self.get_conn()
            
            # Extract folder path and filename from box_path
            box_directory, filename = os.path.split(box_path.strip('/'))
            box_directory = '/' + box_directory if box_directory else '/'

            if not filename:
                return BoxResult(
                    error_message="Must provide full path to Box file including filename"
                )
            
            # Get folder ID for the destination path
            folder_result = self.get_folder_id(box_directory)
            
            if not folder_result.success or folder_result.id is None:
                return BoxResult(
                    error_message=folder_result.error_message or f"Could not find destination folder: {box_directory}"
                )
            
            folder_id = folder_result.id
            
            # Check if file already exists and perform update if it does
            existing_items = client.folder(folder_id=folder_id).get_items()
            file_already_exists = False
            existing_file_id = None
            
            for item in existing_items:
                if item.name == filename:
                    if item.type == 'File':
                        file_already_exists = True
                        existing_file_id = item.id
                    elif item.type == 'Folder':
                        # If a folder with the same name exists, we cannot upload a file with that name
                        return BoxResult(
                            error_message=f"A folder with the name '{filename}' already exists in the destination path."
                        )
                    break

            if file_already_exists and existing_file_id:
                uploaded_file = client.file(file_id=existing_file_id).update_contents(local_file_path)
            else:
                uploaded_file = client.folder(folder_id=folder_id).upload(local_file_path, filename)

            file_info = BoxFileInfo(
                name=uploaded_file.name,
                type=uploaded_file.type,
                size=uploaded_file.size,
                created_at=uploaded_file.created_at,
                modified_at=uploaded_file.modified_at,
                path=box_path,
                updated=file_already_exists
            )
            
            return BoxResult(
                id=uploaded_file.id,
                result=file_info
            )
            
        except Exception as e:
            return BoxResult(error_message=f"Error uploading file: {str(e)}")



