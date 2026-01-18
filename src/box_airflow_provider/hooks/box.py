import fnmatch
import logging
import os
from datetime import datetime
from os.path import relpath
from dataclasses import dataclass
from typing import Any, Literal
from collections.abc import Callable

from airflow.hooks.base import BaseHook
from box_sdk_gen import BoxClient, BoxCCGAuth, CCGConfig, FileFull
from box_sdk_gen.managers.uploads import UploadFileAttributes, UploadFileAttributesParentField, UploadFileVersionAttributes
from box_sdk_gen.managers.folders import CreateFolderParent

@dataclass
class BoxFileInfo:
    """
    Information about a Box file.

    The typing of the original Box SDK is far too complicated to be useful,
    so we're defining a simple model.
    """
    object_id: str
    name: str
    type: str
    size: int
    created_at: datetime | None
    modified_at: datetime | None
    path: str
    new: bool

class BoxHook(BaseHook):
    """
    Interact with Box API.

    This uses the official Box SDK for Python.

    :param box_conn_id: The connection ID to use for Box API
    """
    conn_name_attr = "box_conn_id"
    default_conn_name = "box_default"
    conn_type = "box"
    hook_name = "Box"

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
            box_conn_id: str = default_conn_name,
            client: BoxClient | None = None,
            client_factory: Callable[[], BoxClient] | None = None,
    ) -> None:
        super().__init__()
        self.box_conn_id = box_conn_id
        self.client: BoxClient | None = client
        self.client_factory = client_factory

    def get_conn(self) -> BoxClient:
        if self.client_factory is not None:
            return self.client_factory()

        if self.box_conn_id and not self.client:
            conn = self.get_connection(self.box_conn_id)
            extra = conn.get_extra_dejson()
            client_id = extra['client_id']
            client_secret = extra['client_secret']
            enterprise_id = extra['enterprise_id']

            if not client_id or not client_secret or not enterprise_id:
                raise ValueError("Client ID, Client Secret and Enterprise ID must be provided in connection")

            ccg_config = CCGConfig(
                client_id=client_id,
                client_secret=client_secret,
                enterprise_id=enterprise_id,
            )
            auth = BoxCCGAuth(config=ccg_config)

            self.client = BoxClient(auth=auth)
            logging.getLogger("box_sdk_gen").setLevel(logging.WARNING)

        return self.client

    def test_connection(self) -> (bool, str):
        """Test a connection"""
        conn = self.get_conn()
        # Simple call to get_user_me to test the connection
        conn.users.get_user_me()
        return True, "Connection successfully tested"

    def get_item_id(self, path: str, item_type: Literal['file', 'folder']) -> str:
        """
        Get the ID of a Box item (file or folder) based on a path. Raises an exception if the item does not exist.

        :param path: Path to the item (e.g. "/Test/Area" or "/Test/Area/document.pdf")
        :param item_type: Type of item to look for, either 'file' or 'folder'
        :return: The ID of the target item if found
        """
        return self._get_item_id(path, item_type=item_type, return_deepest_existing=False)[0]

    def get_existing_item_id(self, path: str, item_type: Literal['file', 'folder']) -> tuple[str, str]:
        """
        Get the ID of an existing Box item (file or folder) based on a path.

        :param path: Path to the item (e.g. "/Test/Area" or "/Test/Area/document.pdf")
        :param item_type: Type of item to look for, either 'file' or 'folder'
        :return: The ID of the target item if found
        """
        return self._get_item_id(path, item_type=item_type, return_deepest_existing=True)

    def _get_item_id(self, path: str, item_type: Literal['file', 'folder'], return_deepest_existing: bool = False) -> \
            tuple[str, str]:
        """
        Get the ID of a Box item (file or folder) based on a path. Raises an exception if the item does not exist.

        If the item does not exist, it returns the ID of the deepest existing folder as well as the path to that folder.
        :return: A tuple with the ID and None, or the ID of the deepest existing folder and the path to that folder.
        """
        client = self.get_conn()

        current_folder_id = '0'
        current_path = '/'

        clean_path = path.strip('/')
        if not clean_path:
            if item_type == 'folder':
                return current_folder_id, None
            else:
                raise ValueError("Cannot get file ID for root folder path")

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

            items = client.folders.get_folder_items(folder_id=current_folder_id)

            found = False
            for item in items.entries:
                if item.type.value == 'folder' and item.name == folder_name:
                    current_folder_id = item.id
                    current_path = f"{current_path.rstrip('/')}/{folder_name}"
                    found = True
                    break

            if not found:
                if return_deepest_existing:
                    return current_folder_id, current_path
                raise FileNotFoundError(
                    f"Folder '{folder_name}' not found in path '{current_path}'"
                )

        # If looking for a file, find the file in the final folder
        if item_type == 'file' and file_name:
            items = client.folders.get_folder_items(folder_id=current_folder_id)

            for item in items.entries:
                if item.type.value == 'file' and item.name == file_name:
                    return item.id, None

            raise FileNotFoundError(f"File '{file_name}' not found in folder")

        if return_deepest_existing:
            return current_folder_id, current_path
        return current_folder_id, None

    def get_folder_id(self, path: str) -> str:
        """
        Get the ID of a Box folder based on a path.

        :param path: Path to the folder (e.g. "/Test/Area")
        :return: The ID of the target folder if found
        """
        return self.get_item_id(path, 'folder')

    def get_file_id(self, path: str) -> str:
        """
        Get the ID of a Box file based on a path.

        :param path: Path to the file (e.g. "/Test/Area/document.pdf")
        :return: The ID of the target file if found
        """
        return self.get_item_id(path, 'file')

    def get_file_info(self, path: str) -> BoxFileInfo:
        """
        Get the Box file information based on a path.

        :param path: Path to the file (e.g. "/Test/Area/document.pdf")
        :return: The file information if successful
        """
        file_id = self.get_file_id(path)

        client = self.get_conn()
        box_file = client.files.get_file_by_id(file_id=file_id)

        return box_file_to_file_info(box_file)

    def get_file_modified_time(self, path: str) -> tuple[str, str]:
        """
        Get the last modified time of a Box file based on a path.

        :param path: Path to the file (e.g. "/Test/Area/document.pdf")
        :return: The file's ID and last modified time if successful
        """
        file_info = self.get_file_info(path)

        return file_info.object_id, file_info.modified_at

    def get_files_by_pattern(self, path: str, file_pattern: str) -> list[BoxFileInfo]:
        """
        Get a list of Box files matching a pattern in a given folder.

        :param path: Path to the folder (e.g. "/Test/Area")
        :param file_pattern: Pattern to match files against (e.g. "*.pdf")
        :return: A list of matching BoxFileInfo objects
        """
        client = self.get_conn()
        folder_id = self.get_folder_id(path)

        items = client.folders.get_folder_items(folder_id=folder_id, fields=['name', 'type', 'size', 'created_at', 'modified_at', 'path_collection'])

        matching_files = []
        for item in items.entries:
            if item.type.value == 'file' and fnmatch.fnmatch(item.name, file_pattern):
                matching_files.append(box_file_to_file_info(item))

        return matching_files

    def upload_file(self, local_file_path: str, box_path: str) -> BoxFileInfo:
        """
        Upload a file to Box.

        :param local_file_path: Path to the local file to upload
        :param box_path: Destination path in Box (e.g. "/Test/Area/document.pdf")
        :return: The uploaded file information
        """
        client = self.get_conn()

        # Extract folder path and filename from box_path
        box_directory, filename = os.path.split(box_path.strip('/'))
        box_directory = '/' + box_directory if box_directory else '/'

        if not filename:
            raise ValueError("Must provide full path to Box file including filename")

        folder_id = self.create_folder(box_directory)

        # Check if file already exists and perform update if it does
        existing_items = client.folders.get_folder_items(folder_id=folder_id)
        file_already_exists = False
        existing_file_id = None

        for item in existing_items.entries:
            if item.name == filename:
                if item.type.value == 'file':
                    file_already_exists = True
                    existing_file_id = item.id
                elif item.type.value == 'folder':
                    raise ValueError(f"A folder with the name '{filename}' already exists in the destination path.")
                break

        if file_already_exists and existing_file_id:
            with open(local_file_path, 'rb') as file_stream:
                uploaded_file = client.uploads.upload_file_version(
                    file_id=existing_file_id, 
                    attributes=UploadFileVersionAttributes(name=filename), 
                    file=file_stream
                )
        else:
            with open(local_file_path, 'rb') as file_stream:
                uploaded_file = client.uploads.upload_file(
                    attributes=UploadFileAttributes(name=filename, parent=UploadFileAttributesParentField(id=folder_id)), 
                    file=file_stream
                )

        # Get the file info - the response is Files object with entries
        file_info = box_file_to_file_info(uploaded_file.entries[0])
        file_info.new = not file_already_exists

        return file_info

    def create_folder(self, path: str) -> str:
        """
        Create folders recursively based on the given path.

        :param path: The full path of the folder to create (e.g., "/Parent/Child/Grandchild").
        :return: The ID of the final folder created or found.
        """
        client = self.get_conn()

        # Find the deepest existing folder
        (id, expath) = self.get_existing_item_id(path, item_type='folder')

        current_folder_id = id
        current_path = expath or '/'

        # Get the remaining path to create
        remaining_path = relpath(path, current_path).strip('/')

        if remaining_path == '.':
            return current_folder_id

        # Create the remaining folders
        for folder_name in remaining_path.split('/'):
            folder = client.folders.create_folder(name=folder_name, parent=CreateFolderParent(id=current_folder_id))
            current_folder_id = folder.id

        return current_folder_id

    def download_file(self, box_path: str, local_file_path: str) -> BoxFileInfo:
        """
        Download a file from Box and save it to a local file path.

        :param box_path: Path to the file in Box (e.g., "/Test/Area/document.pdf") or a numeric string representing the file ID.
        :param local_file_path: Path to save the downloaded file locally.
        """
        # Check if box_path is a numeric string (file ID)
        if box_path.isdigit():
            file_id = box_path
        else:
            file_id = self.get_file_id(box_path)

        client = self.get_conn()
        box_file = client.files.get_file_by_id(file_id=file_id)
        info = box_file_to_file_info(box_file)

        file_content = client.downloads.download_file(file_id=file_id)
        # The download_file returns a file-like object, use context manager to ensure proper cleanup
        if file_content:
            try:
                with open(local_file_path, "wb") as local_file:
                    local_file.write(file_content.read())
            finally:
                # Ensure the stream is closed even if write fails
                if hasattr(file_content, 'close'):
                    file_content.close()

        return info

    def delete_file(self, box_path: str) -> None:
        """
        Delete a file from Box based on its path.

        :param box_path: Path to the file in Box (e.g., "/Test/Area/document.pdf") or a numeric string representing the file ID.
        """
        # Check if box_path is a numeric string (file ID)
        if box_path.isdigit():
            file_id = box_path
        else:
            file_id = self.get_file_id(box_path)

        client = self.get_conn()
        client.files.delete_file_by_id(file_id=file_id)


def box_file_to_file_info(box_file: FileFull) -> BoxFileInfo:
    """
    Convert a Box file object to a BoxFileInfo object.

    :param box_file: The Box file object to convert.
    :return: A BoxFileInfo object with the file's information.
    """
    # Build the path from path_collection if available
    if hasattr(box_file, 'path_collection') and box_file.path_collection and box_file.path_collection.entries:
        path_parts = [it.name for it in box_file.path_collection.entries[1::]]
        path = '/' + '/'.join(path_parts) + '/' + box_file.name
    else:
        path = '/' + box_file.name
    
    return BoxFileInfo(
        object_id=box_file.id,
        name=box_file.name,
        type=box_file.type.value,
        # Use the size attribute if available, otherwise default to 0
        size=box_file.size if hasattr(box_file, 'size') and box_file.size is not None else 0,
        created_at=box_file.created_at if hasattr(box_file, 'created_at') and box_file.created_at is not None else None,
        modified_at=box_file.modified_at if hasattr(box_file, 'modified_at') and box_file.modified_at is not None else None,
        path=path,
        new=False
    )
