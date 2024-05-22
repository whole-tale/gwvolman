import json
import os

from kubernetes import client, config

from .constants import NFS_PATH, NFS_SERVER, VOLUMES_ROOT


def tale_ingress(params: dict) -> None:
    host = f"{params['host']}.{params['domain']}"
    annotations = {
        "kubernetes.io/ingress.class": params["ingressClass"],
    }
    if params["ingressClass"] == "traefik":
        annotations[
            "traefik.ingress.kubernetes.io/router.middlewares"
        ] = f"{params['deploymentNamespace']}-ssl-header@kubernetescrd"
    elif params["ingressClass"] == "nginx":
        csp = f"frame-ancestors 'self' https://dashboard.{params['domain']};"
        annotations.update(
            {
                "nginx.ingress.kubernetes.io/proxy-read-timeout": "3600",
                "nginx.ingress.kubernetes.io/proxy-send-timeout": "3600",
                "nginx.ingress.kubernetes.io/proxy-http-version": "1.1",
                "nginx.ingress.kubernetes.io/proxy-body-size": "512M",
                "nginx.ingress.kubernetes.io/configuration-snippet": (
                    'more_clear_headers "Content-Security-Policy";\n'
                    f'add_header Content-Security-Policy "{csp}";\n'
                ),
            }
        )

    # Apply the Ingress
    config.load_incluster_config()
    api_instance = client.NetworkingV1Api()
    api_instance.create_namespaced_ingress(
        body=client.V1Ingress(
            metadata=client.V1ObjectMeta(
                name=f"tale-{params['host']}",
                labels={
                    "app": "WholeTale",
                    "component": params["deploymentName"],
                    "instanceId": params["instanceId"],
                },
                namespace=params["deploymentNamespace"],
                annotations=annotations,
            ),
            spec=client.V1IngressSpec(
                tls=[
                    client.V1IngressTLS(
                        hosts=[host],
                    )
                ],
                rules=[
                    client.V1IngressRule(
                        host=host,
                        http=client.V1HTTPIngressRuleValue(
                            paths=[
                                client.V1HTTPIngressPath(
                                    path="/",
                                    path_type="Prefix",
                                    backend=client.V1IngressBackend(
                                        service=client.V1IngressServiceBackend(
                                            name=params["deploymentName"],
                                            port=client.V1ServiceBackendPort(
                                                number=params["instancePort"],
                                            ),
                                        ),
                                    ),
                                ),
                            ],
                        ),
                    ),
                ],
            ),
        ),
        namespace=params["deploymentNamespace"],
    )


def tale_service(params):
    # Apply the service
    config.load_incluster_config()
    api_instance = client.CoreV1Api()
    api_instance.create_namespaced_service(
        body=client.V1Service(
            metadata=client.V1ObjectMeta(
                name=params["deploymentName"],
                labels={
                    "app": "WholeTale",
                    "component": params["deploymentName"],
                    "instanceId": params["instanceId"],
                },
            ),
            spec=client.V1ServiceSpec(
                selector={"app": "WholeTale", "instanceId": params["instanceId"]},
                ports=[
                    client.V1ServicePort(protocol="TCP", port=params["instancePort"])
                ],
            ),
        ),
        namespace=params["deploymentNamespace"],
    )


def compose_volumes(params):
    volumes = [
        client.V1Volume(name="data", empty_dir={}),
    ]

    if params["girderFSMountType"] == "direct":
        volumes.append(
            client.V1Volume(
                name=params["claimName"],
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=params["claimName"]
                ),
            )
        )
        if NFS_SERVER:
            volumes += [
                client.V1Volume(
                    name="workspace",
                    nfs=client.V1NFSVolumeSource(
                        path=os.path.join(NFS_PATH, params["workspaceSubPath"]),
                        server=NFS_SERVER,
                    ),
                ),
                client.V1Volume(
                    name="home",
                    nfs=client.V1NFSVolumeSource(
                        path=os.path.join(NFS_PATH, params["homeSubPath"]),
                        server=NFS_SERVER,
                    ),
                ),
            ]
    else:
        volumes += [
            client.V1Volume(name="workspace", empty_dir={}),
            client.V1Volume(name="home", empty_dir={}),
        ]

    return volumes


def tale_deployment(params):
    config.load_incluster_config()

    volumes = compose_volumes(params)

    mounter_mounts = [
        client.V1VolumeMount(
            mount_path="/data",
            mount_propagation="Bidirectional",
            name="data",
        ),
    ]
    if params["girderFSMountType"] == "direct":
        mounter_mounts.append(
            client.V1VolumeMount(
                mount_path=VOLUMES_ROOT,
                name=params["claimName"],
                read_only=False,
            ),
        )
    else:
        mounter_mounts += [
            client.V1VolumeMount(
                mount_path="/workspace",
                mount_propagation="Bidirectional",
                name="workspace",
                read_only=False,
            ),
            client.V1VolumeMount(
                mount_path="/home",
                mount_propagation="Bidirectional",
                name="home",
                read_only=False,
            ),
        ]

    instance_mounts = [
        client.V1VolumeMount(
            mount_path=os.path.join(params["mountPoint"], "data"),
            mount_propagation="HostToContainer",
            name="data",
        )
    ]

    if params["girderFSMountType"] == "direct" and not NFS_SERVER:
        instance_mounts += [
            client.V1VolumeMount(
                mount_path=os.path.join(params["mountPoint"], "workspace"),
                name=params["claimName"],
                sub_path=params["workspaceSubPath"],
            ),
            client.V1VolumeMount(
                mount_path=os.path.join(params["mountPoint"], "home"),
                name=params["claimName"],
                sub_path=params["homeSubPath"],
            ),
        ]
    else:
        instance_mounts = [
            client.V1VolumeMount(
                mount_path=os.path.join(params["mountPoint"], "workspace"),
                name="workspace",
            ),
            client.V1VolumeMount(
                mount_path=os.path.join(params["mountPoint"], "home"),
                name="home",
            ),
        ]

    # Apply the deployment
    api_instance = client.AppsV1Api()
    api_instance.create_namespaced_deployment(
        body=client.V1Deployment(
            metadata=client.V1ObjectMeta(
                name=params["deploymentName"],
                namespace=params["deploymentNamespace"],
                labels={
                    "app": "WholeTale",
                    "component": params["deploymentName"],
                    "instanceId": params["instanceId"],
                },
            ),
            spec=client.V1DeploymentSpec(
                selector=client.V1LabelSelector(
                    match_labels={
                        "app": "WholeTale",
                        "instanceId": params["instanceId"],
                    }
                ),
                strategy=client.V1DeploymentStrategy(type="Recreate"),
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={
                            "app": "WholeTale",
                            "instanceId": params["instanceId"],
                        }
                    ),
                    spec=client.V1PodSpec(
                        image_pull_secrets=[
                            client.V1LocalObjectReference(name="local-registry-secret")
                        ],
                        containers=[
                            client.V1Container(
                                name="instance",
                                command=params["command"].split(" "),
                                image=params["instanceImage"],
                                ports=[
                                    client.V1ContainerPort(
                                        container_port=params["instancePort"]
                                    )
                                ],
                                volume_mounts=instance_mounts,
                            ),
                            client.V1Container(
                                name="mounter",
                                command=["tini", "--", "/bin/sleep", "infinity"],
                                image=params["mounterImage"],
                                lifecycle=client.V1Lifecycle(
                                    #    post_start=client.V1LifecycleHandler(
                                    #        _exec=client.V1ExecAction(
                                    #            command=["girderfs-mount"],
                                    #        )
                                    #    ),
                                    pre_stop=client.V1LifecycleHandler(
                                        _exec=client.V1ExecAction(
                                            command=["girderfs-umount"],
                                        )
                                    ),
                                ),
                                resources=client.V1ResourceRequirements(
                                    limits={
                                        "smarter-devices/fuse": 1,
                                    },
                                    requests={
                                        "smarter-devices/fuse": 1,
                                    },
                                ),
                                env=[
                                    client.V1EnvVar(
                                        name="GIRDERFS_DEF",
                                        value=json.dumps(params["girderfsDef"]),
                                    ),
                                    client.V1EnvVar(
                                        name="WT_VOLUMES_PATH",
                                        value=VOLUMES_ROOT,
                                    ),
                                ],
                                security_context=client.V1SecurityContext(
                                    allow_privilege_escalation=True,
                                    capabilities=client.V1Capabilities(
                                        add=["SYS_ADMIN"]
                                    ),
                                    privileged=True,
                                ),
                                volume_mounts=mounter_mounts,
                            ),
                        ],
                        volumes=volumes,
                    ),
                ),
            ),
        ),
        namespace=params["deploymentNamespace"],
    )
