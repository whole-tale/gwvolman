"""Base class for tasks. Contains tasks that are independent of the platform this is deployed on"""
import os
import shutil
import socket
import json
import time
import tempfile
import textwrap
import subprocess
import girder_client
import docker

import logging
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from girder_worker.utils import girder_job
from girder_worker.app import app
# from girder_worker.plugins.docker.executor import _pull_image
from .lib.dataone.publish import DataONEPublishProvider

from .utils import \
    HOSTDIR, REGISTRY_USER, REGISTRY_PASS, \
    new_user, _safe_mkdir, _get_api_key, \
    _get_container_config, _get_user_and_instance

from .constants import GIRDER_API_URL, InstanceStatus, ENABLE_WORKSPACES, \
    DEFAULT_USER, DEFAULT_GROUP, MOUNTPOINTS


class TasksBase:
    def create_volume(self, ctx, instanceId: str):
        raise NotImplementedError()

    def launch_container(self, ctx, payload):
        raise NotImplementedError()

    def update_container(self, ctx, instanceId, **kwargs):
        raise NotImplementedError()

    def shutdown_container(self, ctx, instanceId):
        raise NotImplementedError()

    def remove_volume(self, ctx, instanceId):
        raise NotImplementedError()

    def build_tale_image(self, ctx, tale_id):
        raise NotImplementedError()

    def publish(self, ctx, tale, dataone_node, dataone_auth_token, user_id):
        provider = DataONEPublishProvider()
        return provider.publish(tale, ctx.girder_client, dataone_node,
                            dataone_auth_token, ctx.job_manager)

    def import_tale(self, ctx, lookup_kwargs, tale_kwargs, spawn=True):
        """Create a Tale provided a url for an external data and an image Id.

        Currently, this task only handles importing raw data. In the future, it
        should also allow importing serialized Tales.
        """
        if spawn:
            total = 4
        else:
            total = 3

        ctx.job_manager.updateProgress(
            message='Gathering basic info about the dataset', total=total,
            current=1)
        dataId = lookup_kwargs.pop('dataId')
        try:
            parameters = dict(dataId=json.dumps(dataId))
            parameters.update(lookup_kwargs)
            dataMap = ctx.girder_client.get(
                '/repository/lookup', parameters=parameters)
        except girder_client.HttpError as resp:
            try:
                message = json.loads(resp.responseText).get('message', '')
            except json.JSONDecodeError:
                message = str(resp)
            errormsg = 'Unable to register \"{}\". Server returned {}: {}'
            errormsg = errormsg.format(dataId[0], resp.status, message)
            raise ValueError(errormsg)

        if not dataMap:
            errormsg = 'Unable to register \"{}\". Source is not supported'
            errormsg = errormsg.format(dataId[0])
            raise ValueError(errormsg)

        ctx.job_manager.updateProgress(
            message='Registering the dataset in Whole Tale', total=total,
            current=2)
        ctx.girder_client.post(
            '/dataset/register', parameters={'dataMap': json.dumps(dataMap)})

        # Get resulting folder/item by name
        catalog_path = '/collection/WholeTale Catalog/WholeTale Catalog'
        catalog = ctx.girder_client.get(
            '/resource/lookup', parameters={'path': catalog_path})
        folders = ctx.girder_client.get(
            '/folder', parameters={'name': dataMap[0]['name'],
                               'parentId': catalog['_id'],
                               'parentType': 'folder'}
        )
        try:
            resource = folders[0]
        except IndexError:
            items = ctx.girder_client.get(
                '/item', parameters={'folderId': catalog['_id'],
                                     'name': dataMap[0]['name']})
            try:
                resource = items[0]
            except IndexError:
                errormsg = 'Registration failed. Aborting!'
                raise ValueError(errormsg)

        # Try to come up with a good name for the dataset
        long_name = resource['name']
        long_name = long_name.replace('-', ' ').replace('_', ' ')
        shortened_name = textwrap.shorten(text=long_name, width=30)

        user = ctx.girder_client.get('/user/me')
        payload = {
            'authors': user['firstName'] + ' ' + user['lastName'],
            'title': 'A Tale for \"{}\"'.format(shortened_name),
            'dataSet': [
                {
                    'mountPath': '/' + resource['name'],
                    'itemId': resource['_id'],
                    '_modelType': resource['_modelType']
                }
            ],
            'public': False,
            'published': False
        }

        # allow to override title, etc. MUST contain imageId
        payload.update(tale_kwargs)
        tale = ctx.girder_client.post('/tale', json=payload)

        if spawn:
            ctx.job_manager.updateProgress(
                message='Creating a Tale container', total=total, current=3)
            try:
                instance = ctx.girder_client.post(
                    '/instance', parameters={'taleId': tale['_id']})
            except girder_client.HttpError as resp:
                try:
                    message = json.loads(resp.responseText).get('message', '')
                except json.JSONDecodeError:
                    message = str(resp)
                errormsg = 'Unable to create instance. Server returned {}: {}'
                errormsg = errormsg.format(resp.status, message)
                raise ValueError(errormsg)

            while instance['status'] == InstanceStatus.LAUNCHING:
                # TODO: Timeout? Raise error?
                time.sleep(1)
                instance = ctx.girder_client.get(
                    '/instance/{_id}'.format(**instance))
        else:
            instance = None

        ctx.job_manager.updateProgress(
            message='Tale is ready!', total=total, current=total)
        # TODO: maybe filter results?
        return {'tale': tale, 'instance': instance}

    def build_tale_image(self, ctx, tale_id):
        """
        Build docker image from Tale workspace using repo2docker
        and push to Whole Tale registry.
        """

        logging.info('Building image for Tale %s', tale_id)

        tale = ctx.girder_client.get('/tale/%s' % tale_id)

        last_build_time = -1
        try:
            last_build_time = tale['imageInfo']['last_build']
        except KeyError:
            pass

        logging.info('Last build time {}'.format(last_build_time))

        # Only rebuild if files have changed since last build
        if last_build_time > 0:

            workspace_mtime = -1
            try:
                workspace_mtime = tale['workspaceModified']
            except KeyError:
                pass

            if last_build_time > 0 and workspace_mtime < last_build_time:
                print('Workspace not modified since last build. Skipping.')
                return {
                    'image_digest': tale['imageInfo']['digest'],
                    'repo2docker_version': tale['imageInfo']['repo2docker_version'],
                    'last_build': last_build_time
                }

        # Workspace modified so try to build.
        try:
            temp_dir = tempfile.mkdtemp(dir=HOSTDIR + '/tmp')
            logging.info('Copying workspace contents to %s (%s)', temp_dir, tale_id)
            workspace = ctx.girder_client.get('/folder/{workspaceId}'.format(**tale))
            ctx.girder_client.downloadFolderRecursive(workspace['_id'], temp_dir)

        except Exception as e:
            raise ValueError('Error accessing Girder: {}'.format(e))
        except KeyError:
            logging.info('KeyError')
            pass  # no workspace folderId
        except girder_client.HttpError:
            logging.warn("Workspace folder not found for tale: %s", tale_id)
            pass

        cli = docker.from_env(version='1.28')
        cli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
                  registry=self.deployment.registry_url())

        # Use the current time as the image build time and tag
        build_time = int(time.time())

        tag = '{}/{}/{}'.format(urlparse(self.deployment.registry_url()).netloc,
                                tale_id, str(build_time))

        # Image is required for config information
        image = ctx.girder_client.get('/image/%s' % tale['imageId'])

        # TODO: need to configure version of repo2docker
        repo2docker_version = 'wholetale/repo2docker:latest'

        # Build the image from the workspace
        ret = self._build_image(ctx, cli, tale_id, image, tag, temp_dir, repo2docker_version)

        # Remove the temporary directory whether the build succeeded or not
        shutil.rmtree(temp_dir, ignore_errors=True)

        if ret['StatusCode'] != 0:
            # repo2docker build failed
            raise ValueError('Error building tale {}'.format(tale_id))

        # If the repo2docker build succeeded, push the image to our registry
        apicli = docker.APIClient(base_url=self.deployment.docker_url())
        apicli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
                     registry=self.deployment.registry_url())

        # remove clone
        shutil.rmtree(temp_dir, ignore_errors=True)
        logging.info('Pushing image...')
        for line in apicli.push(tag, stream=True):
            print(line.decode('utf-8'))

        # TODO: if push succeeded, delete old image?

        # Get the built image digest
        image = cli.images.get(tag)

        # This is not quite right since the digest will depend on the deployment
        # and it shouldn't. Images saved on the built-in registry should be
        # portable across deployments.
        digest = next((_ for _ in image.attrs['RepoDigests']
                       if _.startswith(urlparse(self.deployment.registry_url()).netloc)), None)

        logging.info('Successfully built image %s' % image.attrs['RepoDigests'][0])

        # Image digest used by updateBuildStatus handler
        return {
            'image_digest': digest,
            'repo2docker_version': repo2docker_version,
            'last_build': build_time
        }

    def _build_image(self, ctx, cli, tale_id, image, tag, temp_dir, repo2docker_version):
        raise NotImplementedError()

    def _create_session(self, ctx, tale):
        if tale.get('dataSet') is not None:
            session = ctx.girder_client.post(
                '/dm/session', parameters={'taleId': tale['_id']})
        elif tale.get('folderId'):  # old format, keep it for now
            data_set = [
                {'itemId': folder['_id'], 'mountPath': '/' + folder['name']}
                for folder in ctx.girder_client.listFolder(tale['folderId'])
            ]
            data_set += [
                {'itemId': item['_id'], 'mountPath': '/' + item['name']}
                for item in ctx.girder_client.listItem(tale['folderId'])
            ]
            session = ctx.girder_client.post(
                '/dm/session', parameters={'dataSet': json.dumps(data_set)})
        else:
            session = {'_id': None}
        return session