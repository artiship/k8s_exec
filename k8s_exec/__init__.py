# coding: utf-8

import traceback
import signal
import os
import sys

from cluster_rebalancer import pick_k8s_cluster
from configuration import *
from exceptions import *
from executor import Executor
from task import load_pod_config, get_pod_manifest
from kube_client import get_k8s_client_by_context
from unbuffered import Unbuffered

sys.stdout = Unbuffered(sys.stdout)


def run(args):
    k8s_start_args = args[2:]
    task_path = os.path.abspath(args[1])
    path, task_id = os.path.split(task_path)
    pod_name = 'k8s-exec-' + str(task_id)
    pod_config_path = task_path + "/pod_config.yaml"
    pod_dest_path = '/opt/' + task_id

    print(f"Task {pod_name} path: {task_path}")

    cluster_context = DEFAULT_CLUSTER_CONTEXT

    try:
        shutdown_hook()

        pod_config = load_pod_config(pod_config_path)
        print(f"Pod {pod_name} config: {pod_config}")

        export_envs = {}
        try:
            exec_config = load_properties(DEFAULT_CONFIG_PATH)
            if pod_config['container']['image'] in exec_config:
                pod_config['container']['image'] = exec_config.get(pod_config['container']['image'])

            if 'cluster' in pod_config and pod_config['cluster']['context'] is not None:
                cluster_context = pod_config['cluster']['context']

            if cluster_context == 'auto':
                cluster_context = pick_k8s_cluster()
                print(f'Pod {pod_name} auto picks cluster {cluster_context}')

            export_envs['CONTAINER_IMAGE'] = pod_config['container']['image']
            export_envs['API_SERVER'] = exec_config.get(get_cluster_server_key(cluster_context))
        except Exception as e:
            print(traceback.format_exc())
            pass

        executor = Executor(get_k8s_client_by_context(cluster_context, DEFAULT_CLUSTER_CONTEXT), DEFAULT_NAMESPACE)
        try:
            executor.create_pod(pod_name, get_pod_manifest(pod_name, pod_config), DEFAULT_POD_TIMEOUT)
        except TimeoutException as e:
            executor.delete_pod_immediately(pod_name)
            print(f'Pod {pod_name} creating timeout in {DEFAULT_POD_TIMEOUT} seconds, retry on cloud...')
            executor.create_pod(pod_name, get_pod_manifest(pod_name, pod_config, True), DEFAULT_POD_TIMEOUT)

        src_start_file = task_path + "/k8s_start.sh"
        dest_start_file = pod_dest_path + "/k8s_start.sh"

        k8s_tmp = task_path + '/k8s'
        os.system('mkdir ' + k8s_tmp + ';cp ' + src_start_file + ' ' + k8s_tmp)
        executor.copy_files(pod_name, k8s_tmp, pod_dest_path)

        k8s_export_env_cmd = "".join(f"export {k}={v};" for k, v in export_envs.items())
        k8s_start_cmd = f"{k8s_export_env_cmd} bash " + dest_start_file + " " + " ".join(k8s_start_args)

        executor.exec_commands(pod_name, [
            k8s_start_cmd
        ])
    except Exception as e:
        print(traceback.format_exc())
        exit(1)
    finally:
        executor.delete_pod(pod_name)


def shutdown_hook():
    def handler(signum, frame):
        print(f'Handling user terminate task by kill -s {signum}.')
        exit(1)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


if __name__ == '__main__':
    run(sys.argv)
