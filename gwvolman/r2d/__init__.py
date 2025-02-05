import logging
import os

from .docker import DockerImageBuilder  # noqa
from .kaniko import KanikoImageBuilder  # noqa
from .remote import RemoteImageBuilder  # noqa

logger = logging.getLogger(__name__)

if os.environ.get("BUILDER_URL"):
    ImageBuilder = RemoteImageBuilder
elif os.environ.get("DEPLOYMENT", "docker") == "k8s":
    ImageBuilder = KanikoImageBuilder
else:
    ImageBuilder = DockerImageBuilder

logger.warning(f"gwvolman:init: Using {ImageBuilder.__name__} as image builder")
