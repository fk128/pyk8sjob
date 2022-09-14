import os
from typing import List, Optional, Dict
from loguru import logger
import kubernetes
import re
from pyk8sjob.config import config
from dataclasses import dataclass


def sanitize_name(name):
    return re.sub("[^0-9a-zA-Z]+", "-", name).lower()


@dataclass
class K8sJobSpec:
    name: str
    image: str = config.default_dkr_image
    ttl_seconds_after_finished: int = 10
    env_vars: Optional[Dict[str, str]] = None
    parallelism: int = 1
    node_selector: Optional[Dict[str, str]] = None
    resources: Optional[Dict[str, Dict[str, str]]] = None
    tolerations: Optional[List[Dict[str, str]]] = None

    @property
    def command(self):
        raise NotImplementedError


class K8sJobsClient:
    def __init__(self, namespace: Optional[str] = None):
        try:
            kubernetes.config.load_incluster_config()
        except:
            kubernetes.config.load_kube_config(context=config.cluster_name)
        self.v1 = kubernetes.client.BatchV1Api()
        self.namespace = namespace or os.environ.get("KUBERNETES_NAMESPACE", "default")

    def _parsed_job_name(self, job_name):
        return sanitize_name(job_name)

    def get_default_env_vars(self):
        names = []
        env_vars = []
        for name in names:
            env_vars.append(
                kubernetes.client.V1EnvVar(name=name, value=os.environ.get(name, None))
            )
        return env_vars

    def prepare_env_vars(self, vars: Optional[Dict[str, str]] = None):
        if vars is None:
            vars = {}
        env_vars = []
        for name, value in vars.items():
            env_vars.append(kubernetes.client.V1EnvVar(name=name, value=value))
        return env_vars

    def create_job_object(
        self,
        job_name: str,
        image: str,
        command: List[str],
        backoff_limit: int = 2,
        ttl_seconds_after_finished: int = 10,
        node_selector: Optional[Dict[str, str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        parallelism: int = 1,
        completions: int = 1,
        resources: Optional[Dict[str, Dict[str, str]]] = None,
        tolerations: Optional[List[Dict[str, str]]] = None,
    ):
        if tolerations is None:
            tolerations = []
        job_name = self._parsed_job_name(job_name)

        container = kubernetes.client.V1Container(
            name=job_name,
            image=image,
            command=command,
            image_pull_policy="Always",
            env=self.prepare_env_vars(env_vars),
            resources=resources,
        )
        # Create and configure a spec section
        template = kubernetes.client.V1PodTemplateSpec(
            metadata=kubernetes.client.V1ObjectMeta(
                labels={"job": job_name}, generate_name="cache"
            ),
            spec=kubernetes.client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                tolerations=[
                    kubernetes.client.V1Toleration(
                        key=toleration["key"],
                        operator=toleration["operator"],
                        effect=toleration["effect"],
                    )
                    for toleration in tolerations
                ],
                node_selector=node_selector,
            ),
        )

        # Create the specification of deployment
        spec = kubernetes.client.V1JobSpec(
            template=template,
            backoff_limit=backoff_limit,
            ttl_seconds_after_finished=ttl_seconds_after_finished,
            parallelism=parallelism,
            completions=completions,
        )
        # Instantiate the job object
        job = kubernetes.client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=kubernetes.client.V1ObjectMeta(name=job_name),
            spec=spec,
        )

        return job

    def get_job_object(self, job_spec: K8sJobSpec):
        return self.create_job_object(
            job_name=self._parsed_job_name(job_spec.name),
            image=job_spec.image,
            command=job_spec.command,
            ttl_seconds_after_finished=job_spec.ttl_seconds_after_finished,
            env_vars=job_spec.env_vars,
            parallelism=job_spec.parallelism,
            completions=job_spec.parallelism,
            node_selector=job_spec.node_selector,
            tolerations=job_spec.tolerations,
        )

    def submit(self, job_spec: K8sJobSpec, dryrun: bool = False):
        try:
            job = self.get_job_object(job_spec)
            if dryrun:
                print(job)
            else:
                api_response = self.v1.create_namespaced_job(
                    body=job, namespace=self.namespace
                )
                logger.info("Job created. status='%s'" % str(api_response.status))
                self.get_status(job_spec.name)

        except Exception as e:
            logger.error(e)

    def get_status(self, job_name):
        job_name = self._parsed_job_name(job_name)
        try:
            api_response = self.v1.read_namespaced_job_status(
                name=job_name, namespace=self.namespace
            )

            return api_response.status
        except kubernetes.client.rest.ApiException as e:
            if e.status != 404:
                raise
            return None

    def delete(self, job_name):
        job_name = self._parsed_job_name(job_name)
        api_response = self.v1.delete_namespaced_job(
            name=job_name,
            namespace=self.namespace,
            body=kubernetes.client.V1DeleteOptions(
                propagation_policy="Foreground", grace_period_seconds=0
            ),
        )
        logger.info("Job deleted. status='%s'" % str(api_response.status))
