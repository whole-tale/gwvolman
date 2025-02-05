from unittest.mock import MagicMock, patch

import pytest
from girder_client import GirderClient
from gwvolman.r2d.remote import RemoteImageBuilder


@pytest.fixture
def image_builder():
    gc = MagicMock(spec=GirderClient)
    gc.get.return_value = {"_id": "imageId", "config": {}}
    gc.urlBase = "https://test.url/api/v1"
    gc.token = "token"
    return RemoteImageBuilder(
        gc,
        tale={"_id": "test_tale_id", "imageId": "imageId"},
        builder_url="https://builder.test.url",
        registry_user="test_user",
        registry_password="test_password",
        registry_url="https://registry.test.url",
    )


@patch("requests.put")
def test_pull_r2d(mock_put, image_builder):
    mock_response = MagicMock()
    mock_response.iter_lines.return_value = [b'{"status": "pulling"}']
    mock_put.return_value = mock_response

    with patch("builtins.print") as mock_print:
        image_builder.pull_r2d()
        mock_print.assert_called_with("pulling")


@patch("requests.put")
def test_push_image(mock_put, image_builder):
    mock_response = MagicMock()
    mock_response.iter_lines.return_value = [b'{"status": "pushing"}']
    mock_put.return_value = mock_response

    with patch("builtins.print") as mock_print:
        image_builder.push_image("test_image:latest")
        mock_print.assert_called_with(b'{"status": "pushing"}')


@patch("requests.post")
def test_run_r2d_success(mock_post, image_builder):
    mock_response = MagicMock()
    mock_response.iter_lines.return_value = [
        b'{"message": "builidng"}',
        b'{"return": {"ret": {"StatusCode": 0}, "digest": "test_digest"}}',
    ]
    mock_post.return_value = mock_response

    ret, digest = image_builder.run_r2d("test_tag")
    assert ret == {"StatusCode": 0}
    assert digest == "test_digest"


@patch("requests.post")
def test_run_r2d_fail_build(mock_post, image_builder):
    mock_response = MagicMock()
    mock_response.iter_lines.return_value = [
        b'{"message": "builidng"}',
        b'{"message": {"error": "something bad happened"}}',
    ]
    mock_post.return_value = mock_response

    ret, digest = image_builder.run_r2d("test_tag")
    assert ret == {"StatusCode": 1, "error": "something bad happened"}
    assert digest is None


@patch("requests.post")
def test_run_r2d_fail_other(mock_post, image_builder):
    mock_response = MagicMock()
    mock_response.iter_lines.return_value = [
        b'{"message": "builidng"}',
        b'{"error": "something bad happened"}',
    ]
    mock_post.return_value = mock_response

    ret, digest = image_builder.run_r2d("test_tag")
    assert ret == {"StatusCode": 1, "error": "something bad happened"}
    assert digest is None
