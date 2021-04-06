from kubernetes import client, config
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api


def get_k8s_client_corev1():
    config.load_kube_config()
    c = Configuration()
    c.assert_hostname = False
    Configuration.set_default(c)
    core_v1 = core_v1_api.CoreV1Api()
    return core_v1


def get_k8s_client_by_context(context_name, default_context_name):
    contexts, active_context = config.list_kube_config_contexts()
    if not contexts:
        raise Exception("Cannot find any context in kube-config file.")

    config.load_kube_config()
    c = Configuration()
    c.assert_hostname = False
    Configuration.set_default(c)
    try:
        return client.CoreV1Api(
            api_client=config.new_client_from_config(context=context_name))
    except Exception as e:
        return client.CoreV1Api(
            api_client=config.new_client_from_config(context=default_context_name))