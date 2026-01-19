from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

@pytest.fixture()
def hook():
    from box_airflow_provider.hooks.box import BoxHook

    return BoxHook(box_conn_id="box_default")


@pytest.fixture()
def hook_with_fake(box_fake):
    from box_airflow_provider.hooks.box import BoxHook

    # noinspection PyTypeChecker
    return BoxHook(client_factory=box_fake.client)


def test_get_conn_creates_client_and_caches(hook):
    conn = MagicMock()
    conn.get_extra_dejson.return_value = {
        "client_id": "cid",
        "client_secret": "csecret",
        "enterprise_id": "eid",
    }

    hook.get_connection = MagicMock(return_value=conn)

    c1 = hook.get_conn()
    c2 = hook.get_conn()

    assert c1 is c2
    assert c1.auth.config.client_id == "cid"
    assert c1.auth.config.client_secret == "csecret"
    assert c1.auth.config.enterprise_id == "eid"
    hook.get_connection.assert_called_once_with("box_default")


def test_get_conn_raises_for_missing_credentials(hook):
    conn = MagicMock()
    conn.get_extra_dejson.return_value = {
        "client_id": "",
        "client_secret": "csecret",
        "enterprise_id": "eid",
    }
    hook.get_connection = MagicMock(return_value=conn)

    with pytest.raises(ValueError, match="Client ID, Client Secret and Enterprise ID must be provided"):
        hook.get_conn()


def test_test_connection_succeeds(hook_with_fake):

    ok, msg = hook_with_fake.test_connection()

    assert ok is True
    assert "successfully" in msg.lower()


def test_get_item_id_folder_and_file_resolution(hook_with_fake, box_fake):
    child_id = box_fake.seed_folder("/Parent/Child")
    file_id = box_fake.seed_file("/Parent/Child/file.txt")

    assert hook_with_fake.get_folder_id("/Parent/Child") == child_id
    assert hook_with_fake.get_file_id("/Parent/Child/file.txt") == file_id


def test_get_item_id_missing_folder_raises(hook_with_fake):
    with pytest.raises(FileNotFoundError, match="Folder 'Nope' not found"):
        hook_with_fake.get_folder_id("/Nope")


def test_get_existing_item_id_returns_deepest_existing(hook_with_fake, box_fake):
    a_id = box_fake.seed_folder("/A")

    item_id, existing_path = hook_with_fake.get_existing_item_id("/A/B/C", item_type="folder")
    assert item_id == a_id
    assert existing_path == "/A"


def test_get_item_id_root_folder_ok_and_root_file_error(hook_with_fake):
    assert hook_with_fake.get_folder_id("/") == "0"
    with pytest.raises(ValueError, match="Cannot get file ID for root folder path"):
        hook_with_fake.get_file_id("/")


def test_get_file_info_and_modified_time(hook_with_fake, box_fake):
    box_fake.seed_folder("/X")
    file_id = box_fake.seed_file(
        "/X/doc.pdf",
        size=123,
        modified_at="2025-01-01T00:00:00Z",
    )

    info = hook_with_fake.get_file_info("/X/doc.pdf")
    assert info.object_id == file_id
    assert info.name == "doc.pdf"

    file_id2, mod_time = hook_with_fake.get_file_modified_time("/X/doc.pdf")
    assert file_id2 == file_id
    assert mod_time == "2025-01-01T00:00:00Z"


def test_get_files_by_pattern_filters_and_converts(hook_with_fake, box_fake):
    box_fake.seed_folder("/data/sub")
    a_id = box_fake.seed_file("/data/a.txt")
    box_fake.seed_file("/data/b.csv")

    files = hook_with_fake.get_files_by_pattern("/data", "*.txt")
    assert [f.object_id for f in files] == [a_id]
    assert files[0].path.endswith("/a.txt")


def test_upload_file_new_file(tmp_path, hook_with_fake):
    local = tmp_path / "f.txt"
    local.write_text("hello")

    info = hook_with_fake.upload_file(str(local), "/dest/f.txt")
    assert info.object_id.isdigit()
    assert info.new is True
    assert hook_with_fake.get_file_id("/dest/f.txt") == info.object_id


def test_upload_file_updates_existing_file(tmp_path, hook_with_fake, box_fake):
    print("writing")
    local = tmp_path / "f.txt"
    local.write_text("hello")

    print("seed")
    existing_id = box_fake.seed_file("/dest/f.txt", content=b"old")

    print("uploading")
    info = hook_with_fake.upload_file(str(local), "/dest/f.txt")
    assert info.object_id == existing_id
    assert info.new is False

    downloaded = tmp_path / "out.txt"
    print("downloading")
    hook_with_fake.download_file(existing_id, str(downloaded))
    print("downloaded")
    assert downloaded.read_text() == "hello"


def test_upload_file_raises_when_folder_conflicts(tmp_path, hook_with_fake, box_fake):
    local = tmp_path / "f"
    local.write_text("x")

    box_fake.seed_folder("/dest/f")

    with pytest.raises(ValueError, match="folder with the name 'f' already exists"):
        hook_with_fake.upload_file(str(local), "/dest/f")


def test_create_folder_creates_remaining_path(hook_with_fake, box_fake):
    box_fake.seed_folder("/A")
    folder_id = hook_with_fake.create_folder("/A/B/C")
    assert hook_with_fake.get_folder_id("/A/B/C") == folder_id


def test_create_folder_noop_when_exists(hook_with_fake, box_fake):
    existing_id = box_fake.seed_folder("/A/B")

    folder_id = hook_with_fake.create_folder("/A/B")
    assert folder_id == existing_id


def test_download_file_by_id_writes_and_closes_stream(tmp_path, hook_with_fake, box_fake):
    file_id = box_fake.seed_file("/x.bin", content=b"abc")

    dest = tmp_path / "out.bin"
    info = hook_with_fake.download_file(file_id, str(dest))
    assert dest.read_bytes() == b"abc"
    assert info.object_id == file_id
    assert box_fake.last_download_stream is not None
    assert box_fake.last_download_stream.closed is True


def test_download_file_by_path_uses_get_file_id(tmp_path, hook_with_fake, box_fake):
    box_fake.seed_folder("/p")
    box_fake.seed_file("/p/f", content=b"x")

    dest = tmp_path / "out"
    hook_with_fake.download_file("/p/f", str(dest))
    assert dest.read_bytes() == b"x"


def test_delete_file_by_id_and_by_path(hook_with_fake, box_fake):
    file_id = box_fake.seed_file("/x/y")

    hook_with_fake.delete_file(file_id)
    with pytest.raises(KeyError):
        box_fake.get_file_by_id(file_id)

    file_id2 = box_fake.seed_file("/x/z")
    hook_with_fake.delete_file("/x/z")
    with pytest.raises(KeyError):
        box_fake.get_file_by_id(file_id2)


def test_box_file_to_file_info_path_collection_builds_path(box_fake):
    from box_airflow_provider.hooks.box import box_file_to_file_info

    file_id = box_fake.seed_file("/A/B/n.txt", size=0)
    box_file = box_fake.get_file_by_id(file_id)

    info = box_file_to_file_info(box_file)
    assert info.path == "/A/B/n.txt"
    assert info.size == 0
