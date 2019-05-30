"""A set of WT related Girder tasks."""
import os
import shutil
import socket
import json
import time
import tempfile
import textwrap
import subprocess
import docker
import shlex
import base64
from docker.errors import DockerException
import girder_client
from .tasks_base import TasksBase
import kubernetes

import logging
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from girder_worker.utils import girder_job
from girder_worker.app import app
# from girder_worker.plugins.docker.executor import _pull_image
from .utils import \
    HOSTDIR, REGISTRY_USER, REGISTRY_PASS, DOMAIN, \
    new_user, _safe_mkdir, _get_api_key, \
    _get_container_config, _get_user_and_instance, _render_config,\
    _new_container_name
from .utils_kubernetes import _k8s_create_from_file, \
    _env_name, _env_value

from .constants import GIRDER_API_URL, InstanceStatus, ENABLE_WORKSPACES, \
    DEFAULT_USER, DEFAULT_GROUP, MOUNTPOINTS
from .deployment_kubernetes import KubernetesDeployment

class KubernetesTasks(TasksBase):
    def __init__(self):
        self.deployment = KubernetesDeployment()

    def create_volume(self, ctx, instanceId: str):
        """Create a mountpoint and compose WT-fs."""
        user, instance = _get_user_and_instance(ctx.girder_client, instanceId)
        tale = ctx.girder_client.get('/tale/{taleId}'.format(**instance))

        # Can't use underscores or uppercase because it is a kubernetes name which must be DNS-1123
        # compliant
        vol_name = ("%s-%s-%s" % (tale['_id'], user['login'], new_user(6))).lower()
        claim_name = 'claim-' + vol_name

        _k8s_create_from_file(self.deployment.namespace(),
                              'templates/tale-volume-claim.yaml',
                              {'claimName': claim_name,
                               'instanceId': instanceId,
                               'volumeSize': self.deployment.volsz_workspace()})

        # The docker solution proceeds in downloading the tale files into the newly created
        # volume and creating the rest of the workspace structure.
        # We can't do that here, unless we manually manage the tale container using the
        # docker-in-docker solution, which kinda defeats the purpose of using Kubernetes.
        # Instead, we'll launch the tale and use an init container. For the init container,
        # we'll use a copy of this container (aka, one with gwvolman), but launch a specialized
        # tool to do the work instead of using the celery infrastructure.
        # The specifics are in __build_workspace, which should get called by the init container
        # which, in turn, is specified by _launch_container
        return dict(
            nodeId=None,
            mountPoint=None,
            volumeName=vol_name,
            sessionId=None,
            instanceId=instanceId,
            claimName=claim_name
        )

    def _configure_volumes(self, template_params, session, home_dir, tale):
        # DMS
        if session['_id'] is not None:
            template_params['dmsMountEnabled'] = True
            template_params['dmsSessionId'] = str(session['_id'])
        else:
            template_params["dmsMountEnabled"] = False

        # Home
        template_params['homeDirId'] = str(home_dir['_id'])

        # Workspace
        if ENABLE_WORKSPACES:
            template_params['workspacesEnabled'] = True
            template_params['taleId'] = str(tale['_id'])
        else:
            template_params['workspacesEnabled'] = False

    def _update_params_with_config(self, template_params, config):
        template_params['image'] = config.image
        template_params['command'] = [{'arg': x} for x in shlex.split(config.command)]
        print('config.environment: %s' % config.environment)
        # environment is a list of NAME=value strings
        template_params['environment'] = [{'name': _env_name(x), 'value': _env_value(x)}
                                          for x in config.environment]
        template_params['containerPort'] = config.container_port
        template_params['mountpoint'] = config.target_mount
        if config.cpu_shares is not None:
            template_params['cpuSharesFractionEnabled'] = True
            template_params['cpuSharesFraction'] = config.cpu_shares / 1024.0
        else:
            template_params['cpuSharesFractionEnabled'] = False

        if config.mem_limit is not None:
            template_params['memLimitEnabled'] = True
            template_params['memLimit'] = config.mem_limit
        else:
            template_params['memLimitEnabled'] = False

    def _add_ingress_rule(self, service_name, port, host):
        api = kubernetes.client.ExtensionsV1beta1Api()
        ingress = api.read_namespaced_ingress(self.deployment.ingress_name(),
                                              self.deployment.namespace())

        host_name = host + '.' + DOMAIN

        rule = kubernetes.client.V1beta1IngressRule(
            host=host_name,
            http=kubernetes.client.V1beta1HTTPIngressRuleValue(
                [
                    kubernetes.client.V1beta1HTTPIngressPath(
                        backend=kubernetes.client.V1beta1IngressBackend(
                            service_name=service_name,
                            service_port=port
                        ),
                        path=None # forward all requests on this domain
                    )
                ]
            ))
        ingress.spec.rules.append(rule)

        # want to also update tls
        ingress.spec.tls[0].hosts.append(host_name)

        api.replace_namespaced_ingress(self.deployment.ingress_name(), self.deployment.namespace(),
                                       ingress)
        return 'https://%s:%s' % (host_name, port), host_name

    def _remove_ingress_rule(self, ingress_host):
        api = kubernetes.client.ExtensionsV1beta1Api()
        ingress = api.read_namespaced_ingress(self.deployment.ingress_name(),
                                              self.deployment.namespace())

        index = -1
        for i in range(len(ingress.spec.rules)):
            if ingress.spec.rules[i].host == ingress_host:
                index = i
                break
        if index == -1:
            raise LookupError('Could not find ingress rule for host %s' % ingress_host)
        del ingress.spec.rules[index]
        
        index = -1
        for i in range(len(ingress.spec.tls[0].hosts)):
            if ingress.spec.tls[0].hosts[i] == ingress_host:
                index = i
                break
        if index == -1:
            raise LookupError('Could not find ingress tls entry for host %s' % ingress_host)
        del ingress.spec.tls[0].hosts[index]

        api.replace_namespaced_ingress(self.deployment.ingress_name(), self.deployment.namespace(),
                                       ingress)


    def _create_girder_secret(self, ctx):
        api = kubernetes.client.CoreV1Api()
        create = False

        secrets = api.list_namespaced_secret(self.deployment.namespace(),
                                             field_selector='metadata.name=girder-secret')

        if len(secrets.items) > 1:
            raise LookupError('Multiple girder secrets found!')

        if len(secrets.items) == 0:
            # Create
            api_key = _get_api_key(ctx.girder_client)
            encoded_key = base64.b64encode(bytes(api_key, 'ASCII'))
            secret = kubernetes.client.V1Secret('v1', {'apiKey': str(encoded_key, 'ASCII')},
                                                'Secret',
                                                {'name': 'girder-secret',
                                                 'namespace': self.deployment.namespace()})
            api.create_namespaced_secret(self.deployment.namespace(), secret)

    def launch_container(self, ctx, payload):
        """Launch a container using a Tale object."""
        instanceId = payload['instanceId']
        user, instance = _get_user_and_instance(
            ctx.girder_client, instanceId)
        tale = ctx.girder_client.get('/tale/{taleId}'.format(**instance))

        homeDir = ctx.girder_client.loadOrCreateFolder(
            'Home', user['_id'], 'user')

        self._create_girder_secret(ctx)

        host = _new_container_name()
        deployment_name = 'tale-' + host
        
        # Must match ingress host
        service_name = host

        template_params = {'deploymentName': deployment_name,
                  'serviceName': service_name,
                  'claimName': payload['claimName'],
                  'girderApiUrl': GIRDER_API_URL,
                  'workerImage': self.deployment.worker_image(),
                  'instanceId': instanceId}
        container_config = _get_container_config(ctx.girder_client, tale, self.deployment)
        container_config = _render_config(container_config)
        self._update_params_with_config(template_params, container_config)

        session = self._create_session(ctx, tale)
        self._configure_volumes(template_params, session, homeDir, tale)

        url, ingress_host = self._add_ingress_rule(service_name, container_config.container_port,
                                                   host)
        template_params['ingressHost'] = ingress_host

        # create deployment and service
        _k8s_create_from_file(self.deployment.namespace(), 'templates/tale-deployment.yaml',
                              template_params)

        # wait until task is started
        self._wait_for_pod(instanceId)

        payload['url'] = url
        payload['name'] = service_name
        return payload

    def _wait_for_pod(self, instanceId):
        api = kubernetes.client.CoreV1Api()
        tic = time.time()
        timeout = 30.0

        # wait until task is started
        while time.time() - tic < timeout:
            pods = api.list_namespaced_pod(self.deployment.namespace(),
                                           label_selector='instanceId=%s' % instanceId)
            if len(pods.items) == 0:
                logging.info('_wait_for_pod with instance id %s no match' % instanceId)
            elif len(pods.items) > 1:
                logging.error(
                    '_wait_for_pod %s multiple matches; this should not be happening' %
                    instanceId)
            else:
                pod = pods.items[0]
                if pod.status.phase == 'Running':
                    return
                elif pod.status.phase == 'Pending':
                    # Reset deadline
                    tic = time.time()
                elif pod.status.phase == 'Failed':
                    raise Exception('Pod %s failed. Reason: %s, message: %s' %
                                    (instanceId, pod.status.reason, pod.status.message))
            time.sleep(5)
        raise Exception('Pod %s startup timed out' % instanceId)

    def _ensure_one(self, list, type, instanceId):
        if len(list) == 0:
            # TODO: use exception
            # The girder side happily creates the instance even if launch_container throws
            # an exception, which it shouldn't.
            # raise LookupError('No %s found for instanceId %s' % (type, instanceId))
            logging.warning('No %s found for instanceId %s' % (type, instanceId))
            return None
        if len(list) > 1:
            raise LookupError('Multiple %s found for instanceId %s' % (type, instanceId))

        return list[0]

    def update_container(self, ctx, instanceId, **kwargs):
        user, instance = _get_user_and_instance(ctx.girder_client, instanceId)

        # in principle we should just delete the pod and let kubernetes re-create it

        instanceId = instance['_id']
        api = kubernetes.client.CoreV1Api()
        pods = api.list_namespaced_pod(namespace=self.deployment.namespace(),
                                       label_selector='instanceId=%s' % instanceId)

        pod = self._ensure_one(pods.items, 'pods', instanceId)

        mainContainer = None

        for container in pod.spec.containers:
            if container.name == 'main':
                mainContainer = container

        if mainContainer is None:
            raise LookupError('No main container found for pod %s' % pod.metadata.name)

        # hmm; are we sure we don't allow a different repo?
        oldImage = container.image
        if '/' in oldImage:
            container.image = oldImage[0:oldImage.index('/')] + '/' + kwargs['image']
        else:
            container.image = kwargs['image']

        api.replace_namespaced_pod(name=pod.metadata.name, namespace=self.deployment.namespace(),
                                   body=pod)

        return {'image_digest': container.image}

    def shutdown_container(self, ctx, instanceId):
        """Shutdown a running Tale."""
        logging.info("Shutting down container for instance %s" % instanceId)
        # delete deployment, ingress
        api = kubernetes.client.AppsV1Api()
        deployments = api.list_namespaced_deployment(namespace=self.deployment.namespace(),
                                                     label_selector='instanceId=%s' % instanceId)

        deployment = self._ensure_one(deployments.items, 'deployments', instanceId)

        if deployment is None:
            return

        ingressHost = deployment.metadata.labels['ingressHost']

        api.delete_namespaced_deployment(name=deployment.metadata.name,
                                         namespace=self.deployment.namespace())

        api = kubernetes.client.CoreV1Api()
        services = api.list_namespaced_service(namespace=self.deployment.namespace(),
                                               label_selector='instanceId=%s' % instanceId)
        service = self._ensure_one(services.items, 'services', instanceId)
        if service is not None:
            api.delete_namespaced_service(name=service.metadata.name,
                                          namespace=self.deployment.namespace())


        self._remove_ingress_rule(ingressHost)


    def remove_volume(self, ctx, instanceId):
        """Unmount WT-fs and remove mountpoint."""
        logging.info("Removing volume for instance %s" % instanceId)

        api = kubernetes.client.CoreV1Api()

        pvcs = api.list_namespaced_persistent_volume_claim(namespace=self.deployment.namespace(),
                                                           label_selector='instanceId=%s' %
                                                                          instanceId)
        pvc = self._ensure_one(pvcs, 'pvcs', instanceId)
        api.delete_namespaced_persistent_volume_claim(name=pvc.metadata.name,
                                                      namespace=self.deployment.namespace())


    def _build_image(self, ctx, cli, tale_id, image, tag, temp_dir, repo2docker_version):
        """
        Run repo2docker on the workspace using a shared temp directory. Note that
        this uses the "local" provider.  Use the same default user-id and
        user-name as BinderHub
        """
        r2d_cmd = ('jupyter-repo2docker '
                   '--target-repo-dir="/home/jovyan/work/workspace" '
                   '--template={} --buildpack-name={} '
                   '--user-id=1000 --user-name={} '
                   '--no-clean --no-run --debug '
                   '--image-name {} {}'.format(
                                               image['config']['template'],
                                               image['config']['buildpack'],
                                               image['config']['user'],
                                               tag, temp_dir))

        logging.debug('Calling %s (%s)', r2d_cmd, tale_id)

        # This runs in the docker container within the worker pod
        container = cli.containers.run(
            image=repo2docker_version,
            command=r2d_cmd,
            environment=['DOCKER_HOST=unix://var/run/docker.sock'],
            privileged=True,
            detach=True,
            remove=True,
            volumes={
                '/var/run/docker.sock': {
                    'bind': '/var/run/docker.sock', 'mode': 'rw'
                },
                '/src': {
                    'bind': '/src', 'mode': 'ro'
                }
            }
        )

        # Job output must come from stdout/stderr
        for line in container.logs(stream=True):
            print(line.decode('utf-8'))

        # Since detach=True, then we need to explicitly check for the
        # container exit code
        return container.wait()