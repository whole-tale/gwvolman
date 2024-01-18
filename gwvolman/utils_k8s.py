import os

from kubernetes import client, config


def tale_ingress(params):
    host = f"{params['host']}.{params['domain']}"
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
                annotations={
                    "kubernetes.io/ingress.class": "traefik",
                    "traefik.ingress.kubernetes.io/router.middlewares": (
                        f"{params['deploymentNamespace']}-ssl-header@kubernetescrd"
                    ),
                },
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


def tale_deployment(params):
    config.load_incluster_config()
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
                        image_pull_secrets=[client.V1LocalObjectReference(name="local-registry-secret")],
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
                                volume_mounts=[
                                    client.V1VolumeMount(
                                        mount_path=os.path.join(
                                            params["mountPoint"], "workspace"
                                        ),
                                        name=params["claimName"],
                                        sub_path=params["workspaceSubPath"],
                                    ),
                                    client.V1VolumeMount(
                                        mount_path=os.path.join(
                                            params["mountPoint"], "home"
                                        ),
                                        name=params["claimName"],
                                        sub_path=params["homeSubPath"],
                                    ),
                                    client.V1VolumeMount(
                                        mount_path=os.path.join(
                                            params["mountPoint"], "data"
                                        ),
                                        mount_propagation="HostToContainer",
                                        name="dms",
                                    ),
                                ],
                            ),
                            client.V1Container(
                                name="mounter",
                                command=["tini", "--", "/bin/sleep", "infinity"],
                                image=params["mounterImage"],
                                lifecycle=client.V1Lifecycle(
                                    post_start=client.V1LifecycleHandler(
                                        _exec=client.V1ExecAction(
                                            command=params["mountDms"].split(" "),
                                        )
                                    ),
                                    pre_stop=client.V1LifecycleHandler(
                                        _exec=client.V1ExecAction(
                                            command=["umount", "/data"]
                                        )
                                    ),
                                ),
                                resources=client.V1ResourceRequirements(
                                    limits={
                                        "memory": "256Mi",
                                        "smarter-devices/fuse": 1,
                                    },
                                    requests={
                                        "cpu": "1",
                                        "memory": "128Mi",
                                        "smarter-devices/fuse": 1,
                                    },
                                ),
                                security_context=client.V1SecurityContext(
                                    allow_privilege_escalation=True,
                                    capabilities=client.V1Capabilities(
                                        add=["SYS_ADMIN"]
                                    ),
                                    privileged=True,
                                ),
                                volume_mounts=[
                                    client.V1VolumeMount(
                                        mount_path="/data",
                                        mount_propagation="Bidirectional",
                                        name="dms",
                                    ),
                                    client.V1VolumeMount(
                                        mount_path="/srv/data",  # TODO get from girder
                                        name=params["claimName"],
                                        read_only=True,
                                    ),
                                ],
                            ),
                        ],
                        volumes=[
                            client.V1Volume(
                                name=params["claimName"],
                                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                    claim_name=params["claimName"]
                                ),
                            ),
                            client.V1Volume(name="dms", empty_dir={}),
                        ],
                    ),
                ),
            ),
        ),
        namespace=params["deploymentNamespace"],
    )
