import os
from .docker import DockerImageBuilder  # noqa
from .kaniko import KanikoImageBuilder  # noqa

if os.environ.get("DEPLOYMENT", "docker") == "k8s":
    ImageBuilder = KanikoImageBuilder
else:
    ImageBuilder = DockerImageBuilder
