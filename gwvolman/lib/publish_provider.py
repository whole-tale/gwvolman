
"""
Base class for Whole Tale publishing providers.
"""


class PublishProvider(object):

    def publish(self, tale_id, gc, job_manager):
        """
        Publish the specified tale using the provided authenticated
        Girder client. If provided, use job_manager to report progress.

        :param tale_id:  Tale identifier
        :param gc:  Authenticated Girder client
        :param job_manager:  Optional job manager
        """
        raise NotImplementedError
