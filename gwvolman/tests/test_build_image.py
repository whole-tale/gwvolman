import base64
from girder_client import GirderClient
import mock
import os
import pytest
import docker


def mock_gc_get(path, parameters=None):
    workspace1_root = "/collection/WTworkspace/WTworkspaces/tale1"
    if path.startswith("/image"):
        env = os.path.basename(path)
        if env not in {"jupyter", "stata", "matlab"}:
            raise ValueError(f"Unknown image '{env}'")
        return {
            "_id": env,
            "config": {
                "buildpack": f"{env.capitalize()}BuildPack",
                "user": "jovyan",
            },
        }
    elif path in ("/folder/workspace1"):
        return {
            "_id": "workspace1",
            "updated": "2",
        }
    elif path.startswith("/tale"):
        tale_id = os.path.basename(path)
        images = {"tale1": "jupyter", "tale2": "stata", "tale3": "matlab"}
        try:
            return {
                "_id": tale_id,
                "workspaceId": "workspace1",
                "status": 1,
                "imageId": images[tale_id],
                "imageInfo": {
                    "last_build": 1,
                    "imageId": images[tale_id],
                    "repo2docker_version": "wholetale/r2d_wt",
                },
            }
        except KeyError:
            raise ValueError(f"Unknown tale '{tale_id}'")
    elif path in ("/user/me"):
        return {"login": "user1"}
    elif path == "/resource/workspace1/path":
        return workspace1_root
    elif path == "/resource/lookup":
        if parameters["path"] == os.path.join(workspace1_root, "some_file.txt"):
            return {"_id": "some_file_id", "_modelType": "item"}
        if parameters["path"] == os.path.join(workspace1_root, "some_folder"):
            return {"_id": "some_folder_id", "_modelType": "folder"}


def mock_gc_listItem(folderId, name=None):
    if folderId == "workspace1":
        content = {
            "apt.txt": {"_id": "apt_id", "name": "apt.txt"},
            "install.do": {"_id": "stata_id", "name": "install.do"},
            "toolboxes.txt": {"_id": "matlab_id", "name": "toolboxes.txt"},
        }
        try:
            yield from [content[name]]
        except KeyError:
            yield from []


def mock_gc_downloadItem(itemId, target):
    files = {
        "apt_id": ("apt.txt", "vim"),
        "stata_id": ("install.do", "some_stata_package"),
        "matlab_id": ("toolboxes.txt", "some_matlab_environment"),
        "some_file_id": ("some_file.txt", "some_content"),
    }
    fname, content = files[itemId]
    with open(os.path.join(target, fname), "w") as fp:
        fp.write(content)


def mock_gc_downloadFolderRecursive(folderId, target):
    if folderId == "some_folder_id":
        os.makedirs(os.path.join(target, "some_folder"))
        with open(os.path.join(target, "some_folder", "other_file"), "w") as fp:
            fp.write("Other build file content\n")
    elif folderId == "workspace1":
        for obj_id in ("apt_id", "stata_id", "matlab_id", "some_file_id"):
            mock_gc_downloadItem(obj_id, target)
        mock_gc_downloadFolderRecursive("some_folder_id", target)
    else:
        raise ValueError


def docker_services_get(service_name):
    class MockService:
        def __init__(self, name):
            namespace, service = name.split("_")
            self.namespace = namespace
            self.service = service

        @property
        def attrs(self):
            rule = f"Host(`{self.service}.dev.wholetale.org`)"
            return {
                "Spec": {
                    "Labels": {
                        "com.docker.stack.namespace": self.namespace,
                        f"traefik.http.routers.{self.service}.rule": rule,
                    }
                }
            }

    return MockService(service_name)


def docker_run_r2d_container(**kwargs):
    class MockContainer:
        @staticmethod
        def logs(stream=False):
            yield b"First line of Dockerfile"
            yield b"Second line of Dockerfile"

        @staticmethod
        def wait():
            return {"StatusCode": 0}

    return MockContainer()


@mock.patch("docker.APIClient")
@mock.patch(
    "gwvolman.utils.Deployment.registry_url",
    new_callable=mock.PropertyMock,
    return_value="https://registry.dev.wholetale.org",
)
def test_image_builder(depl, dapicli):
    gc = mock.MagicMock(spec=GirderClient)
    gc.get = mock_gc_get
    gc.listItem = mock_gc_listItem
    gc.downloadItem = mock_gc_downloadItem
    gc.downloadFolderRecursive = mock_gc_downloadFolderRecursive
    tale = {
        "imageId": "jupyter",
        "workspaceId": "workspace1",
        "_id": "tale1",
        "config": {
            "extra_build_files": ["some_file.txt", "some_folder"],
            "targetMount": "/home/jovyan/work",
        },
    }
    with mock.patch("docker.from_env") as dcli:
        dcli.return_value.containers.run = docker_run_r2d_container
        dcli.return_value.services.get = docker_services_get

        from gwvolman.build_utils import ImageBuilder

        with pytest.raises(ValueError) as ex:
            image_builder = ImageBuilder(gc)
        assert ex.match("Only one of 'imageId' and 'tale' can be set")

        with pytest.raises(ValueError) as ex:
            image_builder = ImageBuilder(gc, tale=tale, imageId="jupyter")
        assert ex.match("Only one of 'imageId' and 'tale' can be set")

        image_builder = ImageBuilder(gc, imageId="jupyter")
        assert image_builder.tale["imageId"] == "jupyter"

        image_builder = ImageBuilder(gc, tale=tale)
        tag = image_builder.get_tag()
        assert tag == (
            "registry.dev.wholetale.org/tale/"
            "44056037e7d42cdc02490b8f1ffa5446:8c55f2934afafe894125ab12b1d8943c"
        )
        tag = image_builder.get_tag(force=True)
        assert tag == (
            "registry.dev.wholetale.org/tale/"
            "8001f2ae1f45be76dc9a6c2f4c708eab:8c55f2934afafe894125ab12b1d8943c"
        )
        tale["config"]["extra_build_files"] = ["**"]
        image_builder = ImageBuilder(gc, tale=tale)
        tag = image_builder.get_tag()
        assert tag == (
            "registry.dev.wholetale.org/tale/"
            "44056037e7d42cdc02490b8f1ffa5446:8c55f2934afafe894125ab12b1d8943c"
        )


@mock.patch.dict(
    os.environ, {"MATLAB_FILE_INSTALLATION_KEY": "fake-key"}
)
@mock.patch("docker.APIClient")
@mock.patch(
    "gwvolman.utils.Deployment.registry_url",
    new_callable=mock.PropertyMock,
    return_value="https://registry.dev.wholetale.org",
)
def test_r2d_calls(depl, dapicli):
    gc = mock.MagicMock(spec=GirderClient)
    gc.get = mock_gc_get
    gc.listItem = mock_gc_listItem
    gc.downloadItem = mock_gc_downloadItem
    gc.downloadFolderRecursive = mock_gc_downloadFolderRecursive
    tale = {
        "imageId": "jupyter",
        "workspaceId": "workspace1",
        "_id": "tale1",
        "config": {
            "extra_build_files": ["some_file.txt", "some_folder"],
            "targetMount": "/home/jovyan/work",
        },
    }

    from gwvolman.build_utils import ImageBuilder
    from gwvolman.constants import REPO2DOCKER_VERSION

    with mock.patch("docker.from_env") as dcli:
        dcli.return_value.images.pull.side_effect = docker.errors.NotFound("blah")
        image_builder = ImageBuilder(gc, tale=tale)
        with pytest.raises(ValueError) as ex:
            image_builder.pull_r2d()
        assert ex.match(
            f"Requested r2d image '{REPO2DOCKER_VERSION}' not found."
        )

    with mock.patch("docker.from_env") as dcli:
        mock_container_run = mock.MagicMock(wraps=docker_run_r2d_container)
        dcli.return_value.containers.run = mock_container_run
        dcli.return_value.services.get = docker_services_get

        # Stata
        tale["imageId"] = "stata"
        image_builder = ImageBuilder(gc, tale=tale)
        stata_expected_call = mock.call(
            image=REPO2DOCKER_VERSION,
            command="jupyter-repo2docker --engine dockercli"
            " --config='/wholetale/repo2docker_config.py'"
            " --target-repo-dir='/home/jovyan/work/workspace'"
            " --user-id=1000 --user-name=jovyan --no-clean --no-run --debug"
            f"  --build-arg STATA_LICENSE_ENCODED='{base64.b64encode(b'blah').decode()}'"
            f"  --image-name some_tag {image_builder.build_context}",
            environment=["DOCKER_HOST=unix:///var/run/docker.sock"],
            privileged=True,
            detach=True,
            remove=True,
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
                "/tmp": {"bind": "/tmp", "mode": "ro"},
            },
        )

        mock_open = mock.mock_open(read_data="blah")
        with mock.patch("builtins.open", mock_open):
            ret, _ = image_builder.run_r2d("some_tag", image_builder.build_context)
        mock_container_run.assert_has_calls([stata_expected_call])

        # Matlab
        tale["imageId"] = "matlab"
        image_builder = ImageBuilder(gc, tale=tale)
        matlab_expected_call = mock.call(
            image=REPO2DOCKER_VERSION,
            command="jupyter-repo2docker --engine dockercli"
            " --config='/wholetale/repo2docker_config.py'"
            " --target-repo-dir='/home/jovyan/work/workspace'"
            " --user-id=1000 --user-name=jovyan --no-clean --no-run --debug"
            "  --build-arg FILE_INSTALLATION_KEY=fake-key"
            f"  --image-name some_tag {image_builder.build_context}",
            environment=["DOCKER_HOST=unix:///var/run/docker.sock"],
            privileged=True,
            detach=True,
            remove=True,
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
                "/tmp": {"bind": "/tmp", "mode": "ro"},
            },
        )
        ret, _ = image_builder.run_r2d(
            "some_tag",
            image_builder.build_context,
        )
        mock_container_run.assert_has_calls([matlab_expected_call])

        tale["imageId"] = "jupyter"
        for r2d_tag, engine in [("latest", "--engine dockercli"), ("v1.0", "")]:
            r2d = f"wholetale/repo2docker_wholetale:{r2d_tag}"
            tale["imageInfo"] = {"repo2docker_version": r2d}
            image_builder = ImageBuilder(gc, tale=tale)
            expected_call = mock.call(
                image=r2d,
                command=f"jupyter-repo2docker {engine}"
                " --config='/wholetale/repo2docker_config.py'"
                " --target-repo-dir='/home/jovyan/work/workspace'"
                " --user-id=1000 --user-name=jovyan --no-clean --no-run --debug"
                f"  --image-name some_tag {image_builder.build_context}",
                environment=["DOCKER_HOST=unix:///var/run/docker.sock"],
                privileged=True,
                detach=True,
                remove=True,
                volumes={
                    "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
                    "/tmp": {"bind": "/tmp", "mode": "ro"},
                },
            )
            ret, _ = image_builder.run_r2d(
                "some_tag",
                image_builder.build_context,
            )
            mock_container_run.assert_has_calls([expected_call])


@mock.patch(
    "girder_worker.app.Task.canceled",
    new_callable=mock.PropertyMock,
    return_value=False
)
@mock.patch(
    "gwvolman.utils.Deployment.registry_url",
    new_callable=mock.PropertyMock,
    return_value="https://registry.dev.wholetale.org",
)
def test_build_image_task(deployment, task):
    from gwvolman.tasks import build_tale_image
    from gwvolman.constants import TaleStatus

    gc = mock.MagicMock(spec=GirderClient)
    # gc.get = mock_gc_get
    tale = {
        "imageId": "jupyter",
        "workspaceId": "workspace1",
        "_id": "tale1",
        "config": {"extra_build_files": ["some_file.txt", "some_folder"]},
    }
    build_tale_image.job_manager = mock.MagicMock()

    # Test building on Tale in Error state
    gc.get.side_effect = [
        {"_id": "id", "status": TaleStatus.PREPARING},
        {"_id": "id", "status": TaleStatus.ERROR},
    ]
    build_tale_image.girder_client = gc
    with pytest.raises(ValueError) as ex:
        build_tale_image(tale["_id"], force=False)
    assert ex.match("Cannot build image for a Tale in error state.")

    # Test timeout
    gc.get.side_effect = [
        {"_id": "id", "status": TaleStatus.PREPARING},
        {"_id": "id", "status": TaleStatus.PREPARING},
    ]
    with mock.patch("time.time") as fake_time:
        fake_time.side_effect = [1, 999]
        with pytest.raises(ValueError) as ex:
            build_tale_image(tale["_id"], force=False)
        assert ex.match("Cannot build image. Tale preparing for more than 5 minutes.")

    with mock.patch("gwvolman.tasks.ImageBuilder") as image_builder:
        gc.get.side_effect = [
            {"_id": "id", "status": TaleStatus.READY, "imageInfo": {}},
        ]
        build_tale_image.girder_client = gc
        image_builder.return_value.get_tag.return_value = "some_tag"
        image_builder.return_value.cached_image.return_value = {
            "Descriptor": {"digest": "some_digest"}
        }

        result = build_tale_image(tale["_id"], force=False)
        assert result["image_digest"] == "some_tag@some_digest"
        image_builder.return_value.run_r2d.assert_not_called()

        image_builder.return_value.run_r2d.return_value = ({"StatusCode": 1}, 0)
        gc.get = mock_gc_get
        image_builder.return_value.cached_image.return_value = None
        with pytest.raises(ValueError) as ex:
            result = build_tale_image(tale["_id"], force=False)
        image_builder.return_value.run_r2d.assert_called()
        assert ex.match("Error building tale tale1")

        image_builder.return_value.run_r2d.return_value = ({"StatusCode": 0}, 0)
        image_builder.return_value.cached_image.return_value = None
        image = mock.MagicMock()
        image_builder.return_value.dh.cli.images.get.return_value = image
        image.attrs = {"RepoDigests": ["registry.dev.wholetale.org/foo:tag"]}

        result = build_tale_image(tale["_id"], force=False)
        image_builder.return_value.run_r2d.assert_called()
        assert result["image_digest"] == "registry.dev.wholetale.org/foo:tag"
