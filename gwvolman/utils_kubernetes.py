import os
import yaml
import pkg_resources
import pystache
import kubernetes
import logging
import time

# Can't see an easy way to map these automatically
_K8S_APIS={'v1': kubernetes.client.CoreV1Api(), 'apps/v1': kubernetes.client.AppsV1Api()}

def _k8s_get_method_name(kind):
    name = []
    # PersistentVolumeClain -> create_namespaced_persistent_volume_claim
    for i in range(len(kind)):
        c = kind[i]
        if str.isupper(c):
            name.append('_')
            name.append(str.lower(c))
        else:
            name.append(c)
    return 'create_namespaced' + ''.join(name)

def _k8s_invoke(namespace, api_version, kind, body):
    if api_version not in _K8S_APIS:
        raise NameError('Cannot handle API %s' % api_version)
    cls = _K8S_APIS[api_version]
    method_name = _k8s_get_method_name(kind)
    return getattr(cls, method_name)(body=body, namespace=namespace)

def _k8s_create_from_file(namespace, name, params):
    template = pkg_resources.resource_string(__name__, name)
    body_str = pystache.render(template, params)
    bodies = yaml.safe_load_all(body_str)

    for body in bodies:
        api_version = body['apiVersion']
        kind = body['kind']
        logging.debug('yaml body:')
        logging.debug(body)
        _k8s_invoke(namespace, api_version, kind, body)

def _env_name(s):
    return s[0:s.index('=')]

def _env_value(s):
    return s[s.index('=') + 1:]
