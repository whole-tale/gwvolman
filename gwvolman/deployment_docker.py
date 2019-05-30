from .deployment import Deployment
import docker

from .utils import TRAEFIK_ENTRYPOINT, DOMAIN


class DockerDeployment(Deployment):

    _dashboard_url = None
    _girder_url = None
    _registry_url = None
    _traefik_network = None

    def __init__(self):
        self.docker_client = docker.from_env(version='1.28')

    @property
    def traefik_network(self):
        """str: Name of the overlay network used by traefik for ingress."""
        if self._traefik_network is None:
            try:
                service = self.docker_client.services.get('wt_dashboard')
                self._traefik_network = \
                    service.attrs['Spec']['Labels']['traefik.docker.network']
            except docker.errors.APIError:
                self._traefik_network = 'wt_traefik-net'  # Default...
        return self._traefik_network

    @property
    def dashboard_url(self):
        """str: Dashboard's public url."""
        if self._dashboard_url is None:
            self._dashboard_url = self.get_host_from_traefik_rule('wt_dashboard')
        return self._dashboard_url

    @property
    def girder_url(self):
        """str: Girder's public url."""
        if self._girder_url is None:
            self._girder_url = self.get_host_from_traefik_rule('wt_girder')
        return self._girder_url

    @property
    def registry_url(self):
        """str: Docker Registry's public url."""
        if self._registry_url is None:
            self._registry_url = self.get_host_from_traefik_rule('wt_registry')
        return self._registry_url
    
    def docker_url(self):
        return 'unix://var/run/docker.sock'

    def get_host_from_traefik_rule(self, service_name):
        """Infer service's hostname from traefik frontend rule label

        If services are unavailable (slave node), default to DOMAIN env settting
        """
        try:
            service = self.docker_client.services.get(service_name)
            rule = service.attrs['Spec']['Labels']['traefik.frontend.rule']
            return 'https://' + rule.split(':')[-1].split(',')[0].strip()
        except docker.errors.APIError:
            return '{}://{}.{}'.format(TRAEFIK_ENTRYPOINT, service_name[3:], DOMAIN)
