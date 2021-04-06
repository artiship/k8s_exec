DEFAULT_NAMESPACE = 'default'
DEFAULT_CLUSTER_CONTEXT = 'k8s-cluster-online'
DEFAULT_CONFIG_PATH = "/etc/k8s_exec/conf.properties"
DEFAULT_POD_TIMEOUT = 3 * 60


def load_properties(path, sep='=', comment_char='#'):
    props = {}
    with open(path, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"')
                props[key] = value
    return props


def get_cluster_server_key(cluster_context):
    return "cluster." + cluster_context + ".api-server"
