import os
from .deployment import Deployment

REGISTRY_URL = os.environ.get('REGISTRY_URL')
VOLSZ_WORKSPACE = os.environ.get('VOLSZ_WORKSPACE', '1Gi')
MY_NAMESPACE = os.environ.get('MY_NAMESPACE', 'default')
INGRESS_NAME = os.environ.get('INGRESS_NAME', 'ingress')
WORKER_IMAGE = os.environ.get('WORKER_IMAGE', 'wholetale/gwvolman:latest')
DOMAIN = os.environ.get('DOMAIN', 'dev.wholetale.org')


class KubernetesDeployment(Deployment):
    def docker_url(self):
        return 'tcp://localhost:2375'

    def registry_url(self):
        return REGISTRY_URL

    def volsz_workspace(self):
        return VOLSZ_WORKSPACE

    def namespace(self):
        return MY_NAMESPACE

    def ingress_name(self):
        return INGRESS_NAME

    def worker_image(self):
        return WORKER_IMAGE

    def dashboard_url(self):
        return 'https://dashboard.' + DOMAIN

