"""
Scale down Kubernetes Deployments and Statefulsets
and restore them to their original scale
"""

import argparse
import json
from pprint import pprint
from kubernetes import client, config
from kubernetes.client.rest import ApiException

def parse_memory(memory_string):
    """
    Memory may be in Ki, Mi or Gi
    Convert to Ki and strip the suffix
    """

    if memory_string:
        if memory_string.endswith('Ki'):
            truncated_memory = memory_string.replace('Ki', '')
            integer_memory = int(truncated_memory)
        elif memory_string.endswith('Mi'):
            truncated_memory = memory_string.replace('Mi', '')
            integer_memory = 1024 * int(truncated_memory)
        elif memory_string.endswith('Gi'):
            truncated_memory = memory_string.replace('Gi', '')
            integer_memory = 1024 * 1024 * int(truncated_memory)
        elif isinstance(memory_string, str):
            integer_memory = int(memory_string)
        else:
            integer_memory = memory_string
    else:
        integer_memory = memory_string

    return integer_memory

def parse_cpu(cpustring):
    """
    CPU may be integer, float, m, u or n
    Convert to m and strip the suffix
    """
    if cpustring:
        if cpustring.endswith('m'):
            truncatedcpu = int(cpustring.replace('m', ''))
            integercpu = truncatedcpu
        elif cpustring.endswith('u'):
            truncatedcpu = int(cpustring.replace('u', ''))
            integercpu = int(truncatedcpu / 1024)
        elif cpustring.endswith('n'):
            truncatedcpu = int(cpustring.replace('n', ''))
            integercpu = int(truncatedcpu) / (1024 * 1024)
        elif isinstance(cpustring, str):
            integercpu = 1024 * int(cpustring)
        else:
            integercpu = cpustring
    else:
        integercpu = cpustring

    return integercpu

# pylint: disable=too-many-branches
def main():
    """
    Main function
    """

    # Read in args
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dry-run', help="don't actually scale anything", action='store_true')

    parser.add_argument('-o', '--output', choices=['json', 'csv'], default='json')

    parser.add_argument('-n', '--namespace',
                        help="namespace to operate on", type=str)
    args = parser.parse_args()

    # connect to cluster
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    metrics = client.CustomObjectsApi()

    # List pods in all namespaces
    pods = core_v1.list_pod_for_all_namespaces(watch=False)

    # Establish an output structure
    output = {}

    for pod in pods.items:
        # Set up a dict
        try:
            tmp = output[pod.metadata.namespace]
        except KeyError:
            output[pod.metadata.namespace] = {}

        output[pod.metadata.namespace][pod.metadata.name] = {}

        # Loop on containers in the pod
        for container in pod.spec.containers:
            output[pod.metadata.namespace][pod.metadata.name][container.name] = {}
            if container.resources:
                if container.resources.limits:
                    # Memory in Ki, Mi, or Gi
                    # CPU in m or unltless
                    output[pod.metadata.namespace][pod.metadata.name][container.name]['cpu_limits'] = parse_cpu(container.resources.limits.get('cpu'))
                    output[pod.metadata.namespace][pod.metadata.name][container.name]['memory_limits'] = parse_memory(container.resources.limits.get('memory'))
                if container.resources.requests:
                    output[pod.metadata.namespace][pod.metadata.name][container.name]['cpu_requests'] = parse_cpu(container.resources.requests.get('cpu'))
                    output[pod.metadata.namespace][pod.metadata.name][container.name]['memory_requests'] = parse_memory(container.resources.requests.get('memory'))

        # Get metrics for this pod
        try:
            podmetrics = metrics.get_namespaced_custom_object("metrics.k8s.io", "v1beta1", pod.metadata.namespace, 'pods', pod.metadata.name)
        except ApiException:
            continue
        for container in podmetrics['containers']:
            # Memory always in Ki
            output[pod.metadata.namespace][pod.metadata.name][container['name']]['memory_usage'] = parse_memory(container['usage']['memory'])
            # CPU in n or u
            output[pod.metadata.namespace][pod.metadata.name][container['name']]['cpu_usage'] = parse_cpu(container['usage']['cpu'])

            # Work out some ratios
            if output[pod.metadata.namespace][pod.metadata.name][container['name']].get('cpu_requests'):
                output[pod.metadata.namespace][pod.metadata.name][container['name']]['cpu_requests_ratio'] = int(100 * output[pod.metadata.namespace][pod.metadata.name][container['name']]['cpu_usage'] / output[pod.metadata.namespace][pod.metadata.name][container['name']]['cpu_requests'])

            if output[pod.metadata.namespace][pod.metadata.name][container['name']].get('cpu_limits'):
                output[pod.metadata.namespace][pod.metadata.name][container['name']]['cpu_limits_ratio'] = int(100 * output[pod.metadata.namespace][pod.metadata.name][container['name']]['cpu_usage'] / output[pod.metadata.namespace][pod.metadata.name][container['name']]['cpu_limits'])

            if output[pod.metadata.namespace][pod.metadata.name][container['name']].get('memory_requests'):
                output[pod.metadata.namespace][pod.metadata.name][container['name']]['memory_requests_ratio'] = int(100 * output[pod.metadata.namespace][pod.metadata.name][container['name']]['memory_usage'] / output[pod.metadata.namespace][pod.metadata.name][container['name']]['memory_requests'])

            if output[pod.metadata.namespace][pod.metadata.name][container['name']].get('memory_limits'):
                output[pod.metadata.namespace][pod.metadata.name][container['name']]['memory_limits_ratio'] = int(100 * output[pod.metadata.namespace][pod.metadata.name][container['name']]['memory_usage'] / output[pod.metadata.namespace][pod.metadata.name][container['name']]['memory_limits'])

    if args.output == 'json':
        output_json = json.dumps(output, indent = 4)
        print(output_json)
    elif args.output == 'csv':
        print('cpu_requests,cpu_limits,cpu_usage,cpu_requests_ratio,cpu_limits_ratio,memory_requests,memory_limits,memory_usage,memory_requests_ratio,memory_limits_ratio')
        for ns in output:
            for pod in output[ns]:
                for container in output[ns][pod]:
                    print(f"{ns},{pod},{container},{output[ns][pod][container].get('cpu_requests', '')},{output[ns][pod][container].get('cpu_limits', '')},{output[ns][pod][container].get('cpu_usage', '')},{output[ns][pod][container].get('cpu_requests_ratio', '')},{output[ns][pod][container].get('cpu_limits_ratio', '')},{output[ns][pod][container].get('memory_requests', '')},{output[ns][pod][container].get('memory_limits', '')},{output[ns][pod][container].get('memory_usage', '')},{output[ns][pod][container].get('memory_requests_ratio', '')},{output[ns][pod][container].get('memory_limits_ratio', '')}")


    # Determine whether namespaced or global, and fetch list of Deployments
    #if args.namespace:
    #    # do namespaced
    #    if args.deployments:
    #        try:
    #            deployments = apps_v1.list_namespaced_deployment(
    #                namespace=args.namespace)
    #        except ApiException as e:
    #            print(
    #                f"Exception when calling AppsV1Api->list_namespaced_deployment: {e}\n")
    #    if args.statefulsets:
    #        try:
    #            statefulsets = apps_v1.list_namespaced_stateful_set(
    #                namespace=args.namespace)
    #        except ApiException as e:
    #            print(
    #                f"Exception when calling AppsV1Api->list_namespaced_stateful_set: {e}\n")
    #else:
    #    # do global
    #    if args.deployments:
    #        try:
    #            deployments = apps_v1.list_deployment_for_all_namespaces()
    #        except ApiException as e:
    #            print(
    #                f"Exception when calling AppsV1Api->list_deployment_for_all_namespaces: {e}\n")
    #    if args.statefulsets:
    #        try:
    #            statefulsets = apps_v1.list_stateful_set_for_all_namespaces()
    #        except ApiException as e:
    #            print(
    #                f"Exception when calling AppsV1Api->list_stateful_set_for_all_namespaces: {e}\n")

if __name__ == '__main__':
    main()
