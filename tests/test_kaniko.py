import unittest
from unittest.mock import patch, MagicMock
from gwvolman.r2d.kaniko import KanikoImageBuilder, create_configmap, get_pod_logs


class TestKanikoImageBuilder(unittest.TestCase):
    @patch("gwvolman.r2d.kaniko.client.BatchV1Api")
    @patch("gwvolman.r2d.kaniko.client.CoreV1Api")
    @patch("gwvolman.r2d.kaniko.config")
    @patch("json.dump")
    def test_run_r2d(self, mock_json_dump, mock_config, mock_api, mock_batch_api):
        mock_task = MagicMock()
        mock_api_instance = MagicMock()
        mock_api.return_value = mock_api_instance
        mock_batch_api_instance = MagicMock()
        mock_batch_api.return_value = mock_batch_api_instance
        mock_config.load_incluster_config.return_value = None
        mock_json_dump.return_value = None  # Mock json.dump()

        builder = KanikoImageBuilder(MagicMock(), imageId="123", tale=None, auth=True)
        builder.run_r2d("tag", dry_run=False, task=mock_task)

        mock_config.load_incluster_config.assert_called_once()
        mock_api.assert_called_once()
        mock_api_instance.create_namespaced_config_map.assert_called()
        mock_json_dump.assert_called()  # Assert that json.dump() was called
        mock_batch_api.assert_called_once()

    @patch("gwvolman.r2d.kaniko.client.CoreV1Api")
    def test_create_configmap(self, mock_api):
        mock_api_instance = MagicMock()
        mock_api.return_value = mock_api_instance

        create_configmap(mock_api_instance, "configmap_name", {"data": "data"})

        mock_api_instance.create_namespaced_config_map.assert_called_once_with(
            namespace="wt",
            body={
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "configmap_name"},
                "data": {"data": "data"},
            },
        )

    @patch("gwvolman.r2d.kaniko.client.CoreV1Api")
    def test_get_pod_logs(self, mock_api):
        mock_api_instance = MagicMock()
        mock_api.return_value = mock_api_instance
        mock_api_instance.read_namespaced_pod_log.return_value = [
            b"Using local repo\n",
            b"[Repo2Docker]\n",
            b"Other log\n",
        ]

        state = {"state": MagicMock(), "dry_run": False}
        get_pod_logs(mock_api_instance, "pod_name", "container_name", state)

        mock_api_instance.read_namespaced_pod_log.assert_called_once_with(
            name="pod_name",
            namespace="wt",
            container="r2d",
            follow=True,
            _preload_content=False,
        )
        state["state"].update.assert_called_once_with(b"Other log")


if __name__ == "__main__":
    unittest.main()
