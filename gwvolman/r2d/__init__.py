import os
from .docker import DockerImageBuilder  # noqa

if os.environ.get("ENVIRONMENT", "docker") == "docker":
    ImageBuilder = DockerImageBuilder
else:
    raise NotImplementedError("Only docker is supported")
