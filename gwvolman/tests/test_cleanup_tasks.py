import docker
import mock
from girder_client import GirderClient


@mock.patch("docker.from_env")
def test_check_on_run(dcli):
    from gwvolman.tasks import check_on_run

    container = mock.MagicMock(spec=docker.models.containers.Container)
    container.status = "running"
    dcli.return_value.containers.get.return_value = container

    assert check_on_run({"container_name": "foo"})
    dcli.return_value.containers.get.assert_called_with("foo")

    dcli.return_value.containers.get.side_effect = docker.errors.NotFound(
        "404 Client Error for http+docker://localhost/v1.41/containers/foo/json:"
        'Not Found ("No such container: foo")'
    )
    assert not check_on_run({"container_name": "foo"})


def test_cleanup_run():
    from gwvolman.tasks import cleanup_run

    run_with_only_meta = {
        "_id": "run_id",
        "meta": {
            "volume_created": "volume_created",
            "fs_mounted": "fs_mounted",
            "session_created": "session_created",
            "jobId": "jobId",
        },
    }

    gc = mock.MagicMock(spec=GirderClient)
    gc.get.return_value = run_with_only_meta

    cleanup_run.girder_client = gc
    cleanup_run.job_manager = mock.MagicMock()
    with mock.patch("gwvolman.tasks.RecordedRunCleaner.cleanup") as cleanup:
        cleanup_run("run_id")

        gc.get.assert_called_with("/run/run_id")
        gc.patch.assert_called_with("/run/run_id/status", parameters={"status": 4})
        gc.put.assert_called_with("/job/jobId", parameters={"status": 4})
        cleanup.assert_called_with(canceled=False)
