# Copyright (c) 2016, Data Exploration Lab
# Distributed under the terms of the Modified BSD License.

"""A set of helper routines for WT related tasks."""

from collections import namedtuple
import os
import queue
import random
import re
import requests
import string
import time
import tempfile
import threading
import uuid
import logging
import docker
import datetime
import dateutil.relativedelta as rel

from .constants import (
    LICENSE_PATH,
    MOUNTPOINTS,
    NAMESPACE,
    REPO2DOCKER_VERSION,
    VOLUMES_ROOT,
    GIRDERFS_IMAGE,
)
from .lib.stats_collector import DockerStatsCollectorThread

DOCKER_URL = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")
MAX_FILE_SIZE = os.environ.get("MAX_FILE_SIZE", 200)
DOMAIN = os.environ.get("DOMAIN", "dev.wholetale.org")
TRAEFIK_ENTRYPOINT = os.environ.get("TRAEFIK_ENTRYPOINT", "websecure")
REGISTRY_USER = os.environ.get("REGISTRY_USER", "fido")
REGISTRY_PASS = os.environ.get("REGISTRY_PASS")
MOUNTS = {}
RETRIES = 5
container_name_pattern = re.compile(r"tmp\.([^.]+)\.(.+)\Z")
logger = logging.getLogger(__name__)

PooledContainer = namedtuple("PooledContainer", ["id", "path", "host"])
ContainerConfig = namedtuple(
    "ContainerConfig",
    [
        "buildpack",
        "repo2docker_version",
        "image",
        "command",
        "mem_limit",
        "cpu_shares",
        "container_port",
        "container_user",
        "target_mount",
        "url_path",
        "environment",
        "csp",
    ],
)

SIZE_NOTATION_RE = re.compile(r"^(\d+)([kmg]?b?)$", re.IGNORECASE)
SIZE_TABLE = {
    "": 1,
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
}


def size_notation_to_bytes(size):
    if isinstance(size, int):
        return size
    match = SIZE_NOTATION_RE.match(size)
    if match:
        val, suffix = match.groups()
        return int(val) * SIZE_TABLE[suffix.lower()]
    raise ValueError


class K8SDeployment(object):
    """Container for WT-specific k8s stack deployment configuration."""

    __name__ = "K8SDeployment"
    dashboard_url = f"https://dashboard.{DOMAIN}"
    #girder_url = f"http://{os.environ.get('GIRDER_SERVICE_HOST')}:8080"
    girder_url = f"https://girder.{DOMAIN}"
    registry_url = f"https://registry.{DOMAIN}"
    builder_url = os.environ.get("BUILDER_URL", "https://builder.{DOMAIN}")
    traefik_network = None
    tmpdir_mount = "/tmp"
    namespace = NAMESPACE
    girderfs_mount_type = os.environ.get("GIRDERFS_MOUNT_TYPE", "direct")
    ingress_class = os.environ.get("INGRESS_CLASS", "traefik")
    mounter_image = GIRDERFS_IMAGE


class DockerDeployment(object):
    """Container for WT-specific docker stack deployment configuration.

    This class allows to read and store configuration of services in a WT
    deployment. It's meant to be used as a singleton across gwvolman.
    """
    __name__ = "DockerDeployment"
    _dashboard_url = None
    _girder_url = None
    _registry_url = None
    builder_url = os.environ.get("BUILDER_URL", "https://builder.{DOMAIN}")
    _traefik_network = None
    _tmpdir_mount = None

    def __init__(self):
        self.docker_client = docker.from_env(version="1.28")

    @property
    def tmpdir_mount(self):
        """str: Path to the temporary directory used by gwvolman."""
        if self._tmpdir_mount is None:
            service = self.docker_client.services.get("wt_celery_worker")
            tmpdir = tempfile.gettempdir()
            mounts = service.attrs["Spec"]["TaskTemplate"]["ContainerSpec"]["Mounts"]
            self._tmpdir_mount = next(
                (_["Source"] for _ in mounts if _["Target"] == tmpdir), "/tmp"
            )
        return self._tmpdir_mount

    @property
    def traefik_network(self):
        """str: Name of the overlay network used by traefik for ingress."""
        if self._traefik_network is None:
            try:
                service = self.docker_client.services.get("wt_dashboard")
                self._traefik_network = service.attrs["Spec"]["Labels"][
                    "traefik.docker.network"
                ]
            except docker.errors.APIError:
                self._traefik_network = "wt_traefik-net"  # Default...
        return self._traefik_network

    @property
    def dashboard_url(self):
        """str: Dashboard's public url."""
        if self._dashboard_url is None:
            self._dashboard_url = self.get_host_from_traefik_rule("wt_dashboard")
        return self._dashboard_url

    @property
    def girder_url(self):
        """str: Girder's public url."""
        if self._girder_url is None:
            self._girder_url = self.get_host_from_traefik_rule("wt_girder")
        return self._girder_url

    @property
    def registry_url(self):
        """str: Docker Registry's public url."""
        if self._registry_url is None:
            self._registry_url = self.get_host_from_traefik_rule("wt_registry")
        return self._registry_url

    def get_host_from_traefik_rule(self, service_name):
        """Infer service's hostname from traefik frontend rule label.

        If services are unavailable (slave node), default to DOMAIN env settting
        """
        try:
            service = self.docker_client.services.get(service_name)
            ns = service.attrs["Spec"]["Labels"]["com.docker.stack.namespace"]
            router = service_name.replace("%s_" % ns, "")
            rule = service.attrs["Spec"]["Labels"][
                "traefik.http.routers.%s.rule" % router
            ]
            host = re.search(r"Host\(`(.+)`\)", rule).group(1)
            return "https://" + host
        except docker.errors.APIError:
            return "{}://{}.{}".format("https", service_name[3:], DOMAIN)


if os.environ.get("DEPLOYMENT", "docker") == "k8s":
    DEPLOYMENT = K8SDeployment()
else:
    DEPLOYMENT = DockerDeployment()
logger.warning(f"gwvolman:init: Using {DEPLOYMENT.__name__} as a Deployment backend")


def sample_with_replacement(a, size):
    """Get a random path."""
    return "".join([random.SystemRandom().choice(a) for x in range(size)])


def new_user(size):
    """Get a random path."""
    return sample_with_replacement(string.ascii_letters, size)


def _safe_mkdir(dest):
    try:
        os.mkdir(dest)
    except OSError as e:
        if e.errno != 17:
            raise
        logging.warn("Failed to mkdir {}".format(dest))
        pass


def _get_api_key(gc):
    api_key = None
    for key in gc.get("/api_key"):
        if key["name"] == "tmpnb" and key["active"]:
            api_key = key["key"]

    if api_key is None:
        api_key = gc.post("/api_key", data={"name": "tmpnb", "active": True})["key"]
    return api_key


def _get_user_and_instance(girder_client, instanceId):
    user = girder_client.get("/user/me")
    if user is None:
        logging.warn("Bad gider token")
        raise ValueError
    instance = girder_client.get("/instance/" + instanceId)
    return user, instance


def _get_container_config(gc, tale):
    if tale is None:
        container_config = {}  # settings['container_config']
    else:
        image = gc.get("/image/%s" % tale["imageId"])
        tale_config = image["config"] or {}
        if tale.get("config"):
            tale_config.update(tale["config"])

        image_info = tale.get("imageInfo", {})
        digest = image_info.get("digest")
        repo2docker_version = image_info.get("repo2docker_version", REPO2DOCKER_VERSION)

        try:
            mem_limit = size_notation_to_bytes(tale_config.get("memLimit", "2g"))
        except (ValueError, TypeError):
            mem_limit = 2 * 1024**3
        container_config = ContainerConfig(
            buildpack=tale_config.get("buildpack"),
            repo2docker_version=repo2docker_version,
            command=tale_config.get("command"),
            container_port=tale_config.get("port"),
            container_user=tale_config.get("user"),
            cpu_shares=tale_config.get("cpuShares"),
            environment=tale_config.get("environment"),
            image=digest,
            mem_limit=mem_limit,
            target_mount=tale_config.get("targetMount"),
            url_path=tale_config.get("urlPath"),
            csp=tale_config.get("csp"),
        )
    return container_config


def _launch_container(volume_info, container_config, gc):
    token = uuid.uuid4().hex
    # command
    if container_config.command:
        rendered_command = container_config.command.format(
            base_path="",
            port=container_config.container_port,
            ip="0.0.0.0",
            token=token,
        )
    else:
        rendered_command = None

    if container_config.url_path:
        rendered_url_path = container_config.url_path.format(token=token)
    else:
        rendered_url_path = ""

    logging.info("config = " + str(container_config))
    logging.info("command = " + str(rendered_command))
    cli = docker.from_env(version="1.28")
    cli.login(
        username=REGISTRY_USER, password=REGISTRY_PASS, registry=DEPLOYMENT.registry_url
    )
    # Fails with: 'starting container failed: error setting
    #              label on mount source ...: read-only file system'
    # mounts = [
    #     docker.types.Mount(type='volume', source=volumeName, no_copy=True,
    #                        target=container_config.target_mount)
    # ]

    # inject Girder token into the container
    environment = container_config.environment or []
    environment += [f"GIRDER_TOKEN={gc.token}", f"GIRDER_API_URL={gc.urlBase}"]

    source_mount = os.path.join(VOLUMES_ROOT, "mountpoints", volume_info["volumeName"])
    mounts = []
    volumes = _get_container_volumes(source_mount, container_config, MOUNTPOINTS)
    user = gc.get("/user/me")
    volumes[os.path.join(VOLUMES_ROOT, f"homes/{user['login'][0]}/{user['login']}")] = {
        "bind": os.path.join(container_config.target_mount, "home"), "mode": "rw"
    }
    tale = gc.get("/tale/%s" % volume_info["taleId"])
    volumes[os.path.join(VOLUMES_ROOT, f"workspaces/{tale['_id'][0]}/{tale['_id']}")] = {
        "bind": os.path.join(container_config.target_mount, "workspace"), "mode": "rw"
    }

    for source in volumes:
        mounts.append(
            docker.types.Mount(
                type="bind", source=source, target=volumes[source]["bind"]
            )
        )

    host = 'tmp-{}'.format(new_user(12).lower())
    environment.append(f"TMP_URL={host}.{DOMAIN}")

    # https://github.com/containous/traefik/issues/2582#issuecomment-354107053
    endpoint_spec = docker.types.EndpointSpec(mode="vip")

    # Use the specified CSP for iframes or default to deployed host
    csp = ""
    if container_config.csp:
        csp = container_config.csp
    else:
        csp = "frame-ancestors 'self' {}".format(DEPLOYMENT.dashboard_url)

    traefik_loadbalancer_prefix = f"traefik.http.services.{host}.loadbalancer"

    fqdn = f"{host}.{DOMAIN}"

    service = cli.services.create(
        container_config.image,
        command=rendered_command,
        labels={
            f"{traefik_loadbalancer_prefix}.server.port": str(
                container_config.container_port
            ),
            "traefik.enable": "true",
            "traefik.http.routers.%s.rule" % host: "Host(`{}.{}`)".format(host, DOMAIN),
            "traefik.http.routers.%s.entrypoints" % host: TRAEFIK_ENTRYPOINT,
            "traefik.http.routers.%s.tls" % host: "true",
            (
                f"traefik.http.middlewares.{host}"
                "-csp.headers.customresponseheaders.Content-Security-Policy"
            ): csp,
            f"{traefik_loadbalancer_prefix}.passhostheader": "true",
            "traefik.http.routers.%s.middlewares" % host: "girder, %s-csp" % host,
            "traefik.docker.network": DEPLOYMENT.traefik_network,
            "wholetale.instanceId": volume_info["instanceId"],
            "wholetale.taleId": volume_info["taleId"],
        },
        env=environment,
        mode=docker.types.ServiceMode("replicated", replicas=1),
        networks=[DEPLOYMENT.traefik_network],
        name=host,
        hosts={fqdn: "host-gateway"},
        mounts=mounts,
        endpoint_spec=endpoint_spec,
        constraints=["node.id == {}".format(volume_info["nodeId"])],
        resources=docker.types.Resources(mem_limit=container_config.mem_limit),
        restart_policy=docker.types.RestartPolicy(condition="none"),
    )

    # Wait for the server to launch within the container before adding it
    # to the pool or serving it to a user.
    # _wait_for_server(host_ip, host_port, path) # FIXME

    url = "{proto}://{host}.{domain}/{path}".format(
        proto="https", host=host, domain=DOMAIN, path=rendered_url_path
    )

    return service, {"url": url}


def _get_container_volumes(mountpoint, container_config, directories):
    volumes = {}
    for path in directories:
        source = os.path.join(mountpoint, path)
        target = os.path.join(container_config.target_mount, path)
        mode = "rw" if path in ("workspace", "home") else "ro"
        volumes[source] = {"bind": target, "mode": mode}

    if container_config.buildpack:
        # Mount the MATLAB and Stata runtime licenses
        if container_config.buildpack == "MatlabBuildPack":
            volumes[LICENSE_PATH] = {"bind": "/licenses"}
        elif container_config.buildpack == "StataBuildPack":
            # Weekly license expires each Sunday and is provided
            # in the format stata.YYYYMMDD.lic where YYYYMMDD is the
            # license expiration date.
            source_path = _get_stata_license_path()
            volumes[source_path] = {"bind": "/usr/local/stata/stata.lic"}
    return volumes


class DummyTask:
    canceled = False


def stop_container(container: docker.models.containers.Container):
    try:
        container.stop()
    except requests.exceptions.ReadTimeout:
        tries = 10
        while tries > 0:
            container.reload()
            if container.status == "exited":
                break
        if container.status != "exited":
            logging.error(f"Unable to stop container: {container.id}")
    except docker.errors.NotFound:
        logging.warning(f"Container {container.id} was already gone.")
    except docker.errors.DockerException as dex:
        logging.error(dex)
        raise


def _recorded_run(cli, mountpoint, container_config, tag, entrypoint, name, task=None):
    def logging_worker(log_queue, container):
        for line in container.logs(stream=True):
            log_queue.put(line.decode("utf-8").strip(), block=False)

    task = task or DummyTask
    log_queue = queue.Queue()
    print("Starting recorded run")

    # Configure container volumes for recorded run
    volumes = _get_container_volumes(
        mountpoint, container_config, ["data", "workspace"]
    )

    # TODO: use run config, not entrypoint
    run_cmd = f"sh {entrypoint}"

    print("Running Tale with command: " + run_cmd)
    print("Running image: " + tag)

    cli.images.pull(tag)

    container = cli.containers.create(
        image=tag,
        command=run_cmd,
        detach=True,
        name=name,
        volumes=volumes,
        working_dir=os.path.join(container_config.target_mount, "workspace"),
    )

    logging_thread = threading.Thread(
        target=logging_worker, args=(log_queue, container)
    )

    workspace_path = os.path.join(mountpoint, "workspace")
    dstats_tmppath = os.path.join(workspace_path, ".docker_stats")
    stats_thread = DockerStatsCollectorThread(container, dstats_tmppath)

    # Job output must come from stdout/stderr
    container.start()
    stats_thread.start()
    logging_thread.start()

    try:
        container = cli.containers.get(container.id)
        while container.status == "running":
            while not log_queue.empty():
                print(log_queue.get_nowait(), flush=True)
            if task.canceled:
                stop_container(container)
                break
            time.sleep(1)
            container = cli.containers.get(container.id)
    except docker.errors.NotFound:
        pass

    stats_thread.join()
    while not log_queue.empty():
        print(log_queue.get_nowait())
    logging_thread.join()

    if task.canceled:
        ret = {"StatusCode": -123}
    else:
        ret = container.wait()

    # Dump run std{out,err} and entrypoint used.
    with open(os.path.join(workspace_path, ".stdout"), "wb") as fp:
        fp.write(container.logs(stdout=True, stderr=False))
    with open(os.path.join(workspace_path, ".stderr"), "wb") as fp:
        fp.write(container.logs(stdout=False, stderr=True))
    with open(os.path.join(workspace_path, ".entrypoint"), "w") as fp:
        fp.write(entrypoint)
    try:
        container.remove()
    except docker.errors.NotFound:
        pass

    if not task.canceled and ret["StatusCode"] != 0:
        raise ValueError("Error executing recorded run")

    return ret


def _get_stata_license_path():
    license_date = datetime.date.today() + rel.relativedelta(days=1, weekday=rel.SU)
    return os.path.join(
        LICENSE_PATH, "stata", f"stata.{license_date.strftime('%Y%m%d')}.lic"
    )
