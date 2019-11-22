"""
Base class for Whole Tale publishing providers.
"""


class NullManager:
    def updateProgress(self, *args, **kwargs):
        pass


class PublishProvider(object):
    def __init__(self, gc, tale_id, token, draft=False, job_manager=None):
        """
        Initialize PublishProvider

        :param gc:  Authenticated Girder client
        :param tale_id:  Tale identifier
        :param job_manager:  Optional job manager
        :param token: External Account Token
        """
        self.gc = gc
        self.draft = draft
        self.token = token
        if job_manager is not None:
            self.job_manager = job_manager
        else:
            self.job_manager = NullManager()

        self.tale = self.gc.get("/tale/{}".format(tale_id))
        self.manifest = self.gc.get("/tale/{}/manifest".format(tale_id))

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
