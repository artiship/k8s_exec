# coding: utf-8

import yaml
import os


def get_pod_manifest(pod_name, user_config="", on_cloud=False):
    return {}


def load_pod_config(path):
    if not os.path.exists(path):
        print("Please provide pod_config.yaml for the task.")
        exit(1)

    f = open(path)
    pod_config = yaml.load(f, Loader=yaml.FullLoader)

    if not pod_config['container']['image']:
        print("Container Image is not set.")
        exit(1)

    return pod_config
