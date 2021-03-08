"""
Base class for Whole Tale publishing providers.
"""


class NullManager:
    def updateProgress(self, *args, **kwargs):
        pass


class PublishProvider(object):
    _published = None
    _published_info_index = None

    def __init__(self, gc, tale_id, token, version_id, draft=False, job_manager=None):
        """
        Initialize PublishProvider

        :param gc:  Authenticated Girder client
        :param tale_id:  Tale identifier
        :param token: External Account Token
        :param version_id: The Tale's version that's being published
        :param job_manager:  Optional job manager
        """
        self.gc = gc
        self.draft = draft
        self.token = token
        self.version_id = version_id
        if job_manager is not None:
            self.job_manager = job_manager
        else:
            self.job_manager = NullManager()

        self.tale = self.gc.get("/tale/{}".format(tale_id))
        assert self.tale["description"], "Cannot publish a Tale without a description."
        self.manifest = self.gc.get(
            "/tale/{}/manifest".format(tale_id),
            parameters={"expandFolders": True, "versionId": version_id}
        )

    @property
    def published(self):
        if self._published is not None:
            return self._published

        for i, publish_info in enumerate(self.tale.get("publishInfo", [])):
            if self.resource_server == publish_info.get("repository"):
                self._published = True
                self._published_info_index = i
                break
        else:
            self._published = False
        return self._published

    @property
    def publication_info(self):
        if self.published:
            return self.tale["publishInfo"][self._published_info_index]

    @property
    def access_token(self):
        return self.token["access_token"]

    @property
    def resource_server(self):
        return self.token["resource_server"]

    def publish(self):
        """
        Publish the specified tale using the provided authenticated
        Girder client. If provided, use job_manager to report progress.
        """
        raise NotImplementedError

    def rollback(self, *args, **kwargs):
        """
        Rollback publication process if possible.
        """
        pass
