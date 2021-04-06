# coding: utf-8

import time
import tarfile
from tempfile import TemporaryFile
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from k8s_exec import TimeoutException


class Executor:
    def __init__(self, kube, namespace='default'):
        self.kube = kube
        self.namespace = namespace

    def copy_file(self, pod_name, src_file, dest_file):
        exec_command = ['/bin/sh']
        resp = stream(self.kube.connect_get_namespaced_pod_exec,
                      pod_name,
                      self.namespace,
                      command=exec_command,
                      stderr=True, stdin=True,
                      stdout=True, tty=False,
                      _preload_content=False)

        buffer = b''
        with open(src_file, "rb") as file:
            buffer += file.read()

        commands = [
            bytes("cat <<EOF >" + dest_file + "\n", 'utf-8'),
            buffer,
            bytes("\n", 'utf-8'),
            bytes("EOF\n", 'utf-8')
        ]

        while resp.is_open():
            resp.update(timeout=1)
            # if resp.peek_stdout():
            # print(resp.read_stdout())
            if resp.peek_stderr():
                print(resp.read_stderr())
            if commands:
                c = commands.pop(0)
                # print("Running command... %s\n" % c)
                resp.write_stdin(c)
            else:
                break
        resp.close()

    def copy_files(self, pod_name, src_path, dest_path):
        print(f'Pod {pod_name} cp file: src={src_path}, dest={dest_path}')

        try:
            exec_command = ['tar', 'xvf', '-', '-C', '/']
            api_response = stream(self.kube.connect_get_namespaced_pod_exec,
                                  pod_name,
                                  self.namespace,
                                  command=exec_command,
                                  stderr=True, stdin=True,
                                  stdout=True, tty=False,
                                  _preload_content=False)

            with TemporaryFile() as tar_buffer:
                with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
                    tar.add(src_path, dest_path)

                tar_buffer.seek(0)
                commands = [tar_buffer.read()]

                while api_response.is_open():
                    api_response.update(timeout=1)
                    if api_response.peek_stderr():
                        print(api_response.read_stderr())
                    if commands:
                        c = commands.pop(0)
                        api_response.write_stdin(c.decode())
                    else:
                        break
                api_response.close()
        except ApiException as e:
            print("Exception when copying file to the pod%s \n" % e)

    def pod_exists(self, pod_name):
        resp = None
        try:
            resp = self.kube.read_namespaced_pod(pod_name, self.namespace)
        except ApiException as e:
            if e.status != 404:
                print("Unknown error: %s" % e)
                exit(1)

        if not resp:
            return False
        return True

    def create_pod(self, pod_name, pod_manifest, timeout_in_seconds):
        if not self.pod_exists(pod_name):
            print(f"Pod {pod_name} does not exist. Creating it...")
            start_time = int(time.time())

            self.kube.create_namespaced_pod(self.namespace, pod_manifest)
            while True:
                resp = self.kube.read_namespaced_pod(pod_name, self.namespace)

                elapsed = int(time.time()) - start_time
                if elapsed >= timeout_in_seconds:
                    print(f"Pod {pod_name} status:  {resp.status}")
                    raise TimeoutException(Exception(f'Creating timeout in {timeout_in_seconds} seconds'))
                if resp.status.phase != 'Pending':
                    break
                time.sleep(1)
            print(f"Pod {pod_name} is created, pod status: {resp.status.phase}")

    def delete_pod(self, pod_name):
        self.shutdown_hook(pod_name)
        response = self.kube.delete_namespaced_pod(pod_name, self.namespace)
        print(f"Pod {pod_name} is destroyed.")
        return response

    def delete_pod_immediately(self, pod_name):
        self.kube.delete_namespaced_pod(pod_name,
                                        self.namespace,
                                        grace_period_seconds=0,
                                        propagation_policy='Background')
        print(f"Pod {pod_name} is destroyed immediately.")

    def delete_pod_sync(self, pod_name, timeout_in_seconds):
        start_time = int(time.time())
        self.delete_pod(pod_name)

        while self.pod_exists(pod_name):
            elapsed = int(time.time()) - start_time
            if elapsed >= timeout_in_seconds:
                raise TimeoutException(Exception(f"Delete pod timeout in {timeout_in_seconds} seconds"))
            time.sleep(1)

    def shutdown_hook(self, pod_name):
        k8s_terminate_spark_command = "PID=$(ps -ef | grep java | grep spark | grep -v grep | awk '{print $2}');" \
                                      "if [[ '' !=  $PID ]]; then echo \"Killing Spark Application PID: $PID\"; kill -15 $PID; fi"
        try:
            self.exec_command(pod_name, k8s_terminate_spark_command)
        except Exception as e:
            pass

    def exec_commands(self, name, commands):
        for cmd in commands:
            self.exec_command(name, cmd)

    def exec_command(self, name, cmd):
        exec_command = ['/bin/sh', '-c', cmd]

        print(f'Pod {name} execute cmd: {cmd}')
        resp = stream(self.kube.connect_get_namespaced_pod_exec,
                      name,
                      self.namespace,
                      command=exec_command,
                      stderr=True, stdin=False,
                      stdout=True, tty=False,
                      _preload_content=False)

        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                print(resp.read_stdout().strip())
            if resp.peek_stderr():
                print(resp.read_stderr().strip())

        resp.close()

        if resp.returncode != 0:
            raise Exception("Task scripts execute fail")
