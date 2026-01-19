from __future__ import annotations

import sys
from pathlib import Path

from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import io

import pytest


@dataclass
class _FakeBoxItemType:
    value: str


@dataclass
class _FakePathCollection:
    entries: list[Any]


@dataclass
class FakeBoxItem:
    id: str
    name: str
    type: _FakeBoxItemType
    size: int | None = None
    created_at: datetime | None = None
    modified_at: datetime | None = None
    path_collection: _FakePathCollection | None = None


class FakeBoxEnvironment:
    """In-memory Box environment with deterministic IDs.

    This is intentionally a small surface-area fake tailored for unit tests.
    """

    ROOT_ID = "0"
    ROOT_NAME = "All Files"

    def __init__(self) -> None:
        self._next_id = 1
        """ Next available item ID """

        self._items: dict[str, FakeBoxItem] = {}
        """ Map of all item IDs to their metadata """

        self._children: dict[str, dict[str, str]] = {self.ROOT_ID: {}}
        """ Map of all parent IDs to their child IDs """

        self._file_content: dict[str, bytes] = {}
        """ Map of all file IDs to their content """

        self.last_download_stream: io.BytesIO | None = None
        """ Last downloaded file stream, if any """

        # Root folder exists implicitly.
        self._items[self.ROOT_ID] = FakeBoxItem(
            id=self.ROOT_ID,
            name=self.ROOT_NAME,
            type=_FakeBoxItemType("folder"),
            path_collection=_FakePathCollection(entries=[SimpleNamespace(name=self.ROOT_NAME)]),
        )

    def client(self) -> "FakeBoxClient":
        return FakeBoxClient(self)

    def seed_folder(self, path: str) -> str:
        return self._ensure_folder(path)

    def seed_file(
            self,
            path: str,
            *,
            content: bytes = b"",
            size: int | None = None,
            created_at: datetime | None = None,
            modified_at: datetime | None = None,
    ) -> str:
        folder_path, file_name = ("/" + path.strip("/")).rsplit("/", 1)
        folder_id = self._ensure_folder(folder_path)
        file_id = self._create_item(parent_id=folder_id, name=file_name, type_value="file")
        item = self._items[file_id]
        item.size = size if size is not None else len(content)
        item.created_at = created_at
        item.modified_at = modified_at
        item.path_collection = self._build_path_collection(parent_id=folder_id)
        self._file_content[file_id] = content
        return file_id

    def set_file_modified_at(self, file_id: str, modified_at: datetime | None) -> None:
        """Update a seeded file's modified time."""
        item = self._items[file_id]
        item.modified_at = modified_at

    def get_folder_items(self, folder_id: str, *, fields: list[str] | None = None) -> Any:
        children = self._children.get(folder_id, {})
        entries = [self._items[item_id] for item_id in children.values()]
        return SimpleNamespace(entries=entries)

    def get_file_by_id(self, file_id: str) -> FakeBoxItem:
        return self._items[file_id]

    def download_file(self, file_id: str) -> io.BytesIO:
        stream = io.BytesIO(self._file_content.get(file_id, b""))
        self.last_download_stream = stream
        return stream

    def delete_file_by_id(self, file_id: str) -> None:
        item = self._items.get(file_id)
        if item is None:
            return
        # Remove from parent children index
        for parent_id, children in self._children.items():
            for name, cid in list(children.items()):
                if cid == file_id:
                    del children[name]
                    break
        self._items.pop(file_id, None)
        self._file_content.pop(file_id, None)

    def create_folder(self, *, name: str, parent_id: str) -> FakeBoxItem:
        folder_id = self._create_item(parent_id=parent_id, name=name, type_value="folder")
        folder = self._items[folder_id]
        folder.path_collection = self._build_path_collection(parent_id=parent_id)
        return folder

    def upload_file(self, *, name: str, parent_id: str, file_obj: Any) -> Any:
        content = file_obj.read()
        file_id = self._create_item(parent_id=parent_id, name=name, type_value="file")
        item = self._items[file_id]
        item.size = len(content)
        item.path_collection = self._build_path_collection(parent_id=parent_id)
        self._file_content[file_id] = content
        return SimpleNamespace(entries=[item])

    def upload_file_version(self, *, file_id: str, name: str, file_obj: Any) -> Any:
        content = file_obj.read()
        item = self._items[file_id]
        item.name = name
        item.size = len(content)
        self._file_content[file_id] = content
        return SimpleNamespace(entries=[item])

    def _build_path_collection(self, *, parent_id: str) -> _FakePathCollection:
        # Build entries with at least a root element; hook code only reads `.name`.
        parts: list[str] = []
        current_id = parent_id
        while current_id != self.ROOT_ID:
            item = self._items[current_id]
            parts.append(item.name)
            current_id = getattr(item, "_parent_id")
        parts.reverse()
        entries = [SimpleNamespace(name=self.ROOT_NAME)] + [SimpleNamespace(name=p) for p in parts]
        return _FakePathCollection(entries=entries)

    def _ensure_folder(self, path: str) -> str:
        clean = path.strip("/")
        if not clean:
            return self.ROOT_ID
        current_id = self.ROOT_ID
        for part in clean.split("/"):
            if not part:
                continue
            children = self._children.setdefault(current_id, {})
            existing_id = children.get(part)
            if existing_id is not None:
                current_id = existing_id
                continue
            current_id = self._create_item(parent_id=current_id, name=part, type_value="folder")
            folder = self._items[current_id]
            folder.path_collection = self._build_path_collection(parent_id=getattr(folder, "_parent_id"))
        return current_id

    def _create_item(self, *, parent_id: str, name: str, type_value: str) -> str:
        new_id = str(self._next_id)
        self._next_id += 1
        item = FakeBoxItem(id=new_id, name=name, type=_FakeBoxItemType(type_value))
        # Keep parent pointer for path building.
        setattr(item, "_parent_id", parent_id)
        self._items[new_id] = item
        self._children.setdefault(parent_id, {})[name] = new_id
        if type_value == "folder":
            self._children.setdefault(new_id, {})
        return new_id


class _FakeUsersManager:
    """Fake for client.users"""

    def get_user_me(self) -> Any:  # pragma: no cover
        return SimpleNamespace(id="me")


class _FakeFoldersManager:
    """Fake for client.folders"""

    def __init__(self, env: FakeBoxEnvironment) -> None:
        self._env = env

    def get_folder_items(self, *, folder_id: str, fields: list[str] | None = None) -> Any:
        return self._env.get_folder_items(folder_id, fields=fields)

    def create_folder(self, *, name: str, parent: Any) -> FakeBoxItem:
        return self._env.create_folder(name=name, parent_id=parent.id)


class _FakeFilesManager:
    """Fake for client.files"""

    def __init__(self, env: FakeBoxEnvironment) -> None:
        self._env = env

    def get_file_by_id(self, *, file_id: str) -> FakeBoxItem:
        return self._env.get_file_by_id(file_id)

    def delete_file_by_id(self, *, file_id: str) -> None:
        self._env.delete_file_by_id(file_id)


class _FakeUploadsManager:
    """Fake for client.uploads"""

    def __init__(self, env: FakeBoxEnvironment) -> None:
        self._env = env

    def upload_file(self, *, attributes: Any, file: Any) -> Any:
        return self._env.upload_file(name=attributes.name, parent_id=attributes.parent.id, file_obj=file)

    def upload_file_version(self, *, file_id: str, attributes: Any, file: Any) -> Any:
        return self._env.upload_file_version(file_id=file_id, name=attributes.name, file_obj=file)


class _FakeDownloadsManager:
    """Fake for client.downloads"""

    def __init__(self, env: FakeBoxEnvironment) -> None:
        self._env = env

    def download_file(self, *, file_id: str) -> io.BytesIO:
        return self._env.download_file(file_id)


class FakeBoxClient:
    def __init__(self, env: FakeBoxEnvironment) -> None:
        self._env = env
        self.users = _FakeUsersManager()
        self.folders = _FakeFoldersManager(env)
        self.files = _FakeFilesManager(env)
        self.uploads = _FakeUploadsManager(env)
        self.downloads = _FakeDownloadsManager(env)


@pytest.fixture()
def box_fake() -> FakeBoxEnvironment:
    """Reusable fake Box environment with a known empty root (id='0')."""
    return FakeBoxEnvironment()


def pytest_configure() -> None:
    # Ensure `src/` is on `sys.path` so `box_airflow_provider` imports work when running pytest from repo root.
    project_root = Path(__file__).resolve().parents[1]
    src = project_root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
