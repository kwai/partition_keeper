#!/usr/bin/env python3

import pprint
import typing
import utils
import socket
import io
import sys
import time

import google.protobuf.json_format as json_format
import teams.reco_arch.colossusdb.proto.partition_keeper_admin_pb2 as admin_pb
import teams.reco_arch.colossusdb.proto.common_pb2 as common_pb
import teams.reco_arch.colossusdb.proto.partition_service_pb2 as ps_pb

from tabulate import tabulate
from output_handler import OutputHandler

custom_printers = {}


class Printer(OutputHandler):
    SPLITTER_LARGE = "===================="
    SPLITTER_SMALL = "----------"

    def register_printer(func):
        custom_printers[func.__name__] = func

    def print_columns(
        data: dict,
        key: str,
        headers: typing.List[str] = [],
        output: io.IOBase = sys.stdout,
        formatters: typing.Dict[str, typing.Callable[[typing.Any],
                                                     typing.Any]] = {}):
        def _format_item(key, item):
            if key in formatters:
                return formatters[key](item)
            return item

        content = data[key]
        if content is None or len(content) == 0:
            print(key + ': []', file=output)
            return
        print(key, file=output)
        print(Printer.SPLITTER_SMALL, file=output)
        if len(headers) == 0:
            headers = list(content[0].keys())
        rows = []
        for item in content:
            row = []
            for key in headers:
                row.append(_format_item(key, item[key]))
            rows.append(row)

        table = tabulate(rows,
                         headers=headers,
                         tablefmt='plain',
                         numalign='left')
        print(table, file=output)

    def pretty_print_struct(data: dict, output: io.IOBase = sys.stdout):
        rows = []
        for key, value in data.items():
            rows.append([key, value])

        table = tabulate(rows, tablefmt='plain', numalign='left')
        print(table, file=output)

    def pretty_print_struct_list(data: dict,
                                 key: str,
                                 output: io.IOBase = sys.stdout):
        content = data[key]
        if content is None or len(content) == 0:
            print(key + ": []", file=output)
            return

        print(key, file=output)
        for struct in content:
            print(Printer.SPLITTER_SMALL, file=output)
            Printer.pretty_print_struct(struct, output=output)

    @register_printer
    def list_services(result: dict):
        output = io.StringIO()
        for service in result["service_names"]:
            print(service, file=output)
        return output.getvalue()

    @register_printer
    def query_service(result: dict):
        def format_roles(roles: typing.List[int]) -> typing.List[str]:
            if roles is None:
                return []
            return [common_pb.ReplicaRole.Name(r) for r in roles]

        output = io.StringIO()
        service_type = admin_pb.ServiceType.Name(result['service_type'])
        failure_domain_type = "HOST(default_val)"
        if 'failure_domain_type' in result:
            failure_domain_type = admin_pb.NodeFailureDomainType.Name(
                result['failure_domain_type'])
        static_indexed = "False(default_val)"
        if 'static_indexed' in result:
            static_indexed = result['static_indexed']
        use_paz = "False(default_val)"
        if 'use_paz' in result:
            use_paz = result['use_paz']
        service_ksn = "NULL(default_val)"
        if 'service_ksn' in result:
            service_ksn = result['service_ksn']

        table = tabulate(
            [['service_type', service_type],
             ['scheduler_enabled', result['schedule_enabled']],
             ['kess_poller_enabled', result['kess_poller_enabled']],
             [
                 'failure_domain_type',
                 failure_domain_type,
             ],
             ['static_indexed', static_indexed],
             ['use_paz', use_paz]], ['service_ksn', service_ksn],
            tablefmt='plain',
            numalign='left')
        print(table, file=output)
        print(Printer.SPLITTER_LARGE, file=output)
        Printer.print_columns(result,
                              'nodes_hubs',
                              output=output,
                              formatters={"disallowed_roles": format_roles})
        if "sched_opts" in result:
            print(Printer.SPLITTER_LARGE, file=output)
            print("schedule_options", file=output)
            print(Printer.SPLITTER_SMALL, file=output)
            Printer.pretty_print_struct(result['sched_opts'], output)
        return output.getvalue()

    @register_printer
    def list_tables(result: dict):
        output = io.StringIO()
        Printer.print_columns(result,
                              'tables',
                              headers=[
                                  'table_id', 'table_name', 'parts_count',
                                  'belong_to_service', 'kconf_path',
                                  'restore_version'
                              ],
                              output=output)
        return output.getvalue()

    def get_peers(
            peer_info: ps_pb.PartitionPeerInfo, resolve: bool,
            with_statistics_info: bool) -> typing.Tuple[str, str, str, bool]:
        pris = []
        secs = []
        learners = []

        def str_items(items):
            if len(items) > 0:
                return ",".join(items)
            return "-"

        for replica in peer_info.peers:
            statistics_info = []
            promote = replica.ready_to_promote
            name = replica.node.node_name
            if with_statistics_info:
                for key, value in replica.statistics_info.items():
                    statistics_info.append("{}:{}".format(key, value))

            if resolve:
                name = socket.gethostbyname(name)
            if replica.role == common_pb.ReplicaRole.kPrimary:
                if with_statistics_info:
                    pris.append("{}:{}({})".format(name, replica.node.port,
                                                   str_items(statistics_info)))
                else:
                    pris.append("{}:{}".format(name, replica.node.port))
            elif replica.role == common_pb.ReplicaRole.kSecondary:
                if with_statistics_info:
                    secs.append("{}:{}({})".format(name, replica.node.port,
                                                   str_items(statistics_info)))
                else:
                    secs.append("{}:{}".format(name, replica.node.port))
            else:
                if with_statistics_info:
                    learners.append("{}:{}({},{})".format(
                        name, replica.node.port, promote,
                        str_items(statistics_info)))
                else:
                    learners.append("{}:{}({})".format(name, replica.node.port,
                                                       promote))

        return str_items(pris), str_items(secs), str_items(learners)

    def print_partition_by_hubs(partitions,
                                resolve=False,
                                from_partition: int = 0,
                                to_partition: int = 0,
                                output: io.IOBase = sys.stdout):
        def _char_role(role: common_pb.ReplicaRole) -> str:
            if role == common_pb.ReplicaRole.kPrimary:
                return 'P'
            if role == common_pb.ReplicaRole.kSecondary:
                return 'S'
            if role == common_pb.ReplicaRole.kLearner:
                return 'L'
            raise Exception("known role {}".format(role))

        hubs = dict()
        for p in partitions:
            peer_info = ps_pb.PartitionPeerInfo()
            json_format.ParseDict(p, peer_info, ignore_unknown_fields=True)
            for replica in peer_info.peers:
                hubs[replica.hub_name] = 0

        headers = ['id', 'version']
        headers.extend(sorted(hubs.keys()))
        index = 0
        for hub in sorted(hubs.keys()):
            hubs[hub] = index
            index += 1

        rows = []
        id = from_partition
        for p in partitions:
            peer_info = ps_pb.PartitionPeerInfo()
            json_format.ParseDict(p, peer_info, ignore_unknown_fields=True)
            row = [id, peer_info.membership_version]
            row.extend(['-'] * index)
            for replica in peer_info.peers:
                name = replica.node.node_name
                if resolve:
                    name = socket.gethostbyname(name)
                entry = "{}:{}:{}".format(_char_role(replica.role), name,
                                          replica.node.port)
                pos = 2 + hubs[replica.hub_name]
                if row[pos] == '-':
                    row[pos] = entry
                else:
                    row[pos] = row[pos] + "," + entry
            rows.append(row)
            id += 1

        table = tabulate(rows,
                         headers=headers,
                         tablefmt='plain',
                         numalign='left')
        print(table, file=output)

    def print_partition_by_roles(partitions,
                                 resolve=False,
                                 with_statistics_info=False,
                                 from_partition: int = 0,
                                 to_partition: int = 0,
                                 output: io.IOBase = sys.stdout):
        headers = [
            'id', 'version', 'primary', 'secondaries',
            'learners(ready_to_promote)'
        ]
        id = from_partition
        rows = []
        for partition in partitions:
            peer_info = ps_pb.PartitionPeerInfo()
            json_format.ParseDict(partition,
                                  peer_info,
                                  ignore_unknown_fields=True)
            row = [id, peer_info.membership_version]
            row.extend(
                Printer.get_peers(peer_info, resolve, with_statistics_info))
            rows.append(row)
            id += 1

        table = tabulate(rows,
                         headers=headers,
                         tablefmt="plain",
                         numalign='left')
        print(table, file=output)

    def print_recent_executions(tasks, output: io.IOBase = sys.stdout):
        def get_finish_status(task_info):
            if task_info.finish_second == 0:
                return "-"
            else:
                return admin_pb.TaskExecInfo.FinishStatus.Name(
                    task_info.finish_status)

        def get_finish_time(finish_ts):
            if finish_ts == 0:
                return '-'
            return time.strftime("%D-%H:%M:%S", time.localtime(finish_ts))

        headers = [
            'session id',
            'start time',
            'finish time',
            'finish status',
            'args',
        ]
        rows = []

        if tasks:
            for task in reversed(tasks):
                task_info = admin_pb.TaskExecInfo()
                json_format.ParseDict(task,
                                      task_info,
                                      ignore_unknown_fields=True)
                start_time = time.strftime(
                    "%D-%H:%M:%S", time.localtime(task_info.start_second))
                row = [
                    task_info.session_id, start_time,
                    get_finish_time(task_info.finish_second),
                    get_finish_status(task_info), task_info.args
                ]
                rows.append(row)

        table = tabulate(rows,
                         headers=headers,
                         tablefmt="plain",
                         numalign='left')
        print(table, file=output)

    def print_task_current_executions(partitions,
                                      output: io.IOBase = sys.stdout):
        if partitions:
            headers = [
                'partition id',
                'send_host(progress)',
                'finished',
            ]
            rows = []
            for partition in partitions:
                partition_info = admin_pb.TaskPartExecution()
                json_format.ParseDict(partition,
                                      partition_info,
                                      ignore_unknown_fields=True)
                partition_id = partition_info.partition_id
                finish = partition_info.finish
                send_host = ""
                for replica_info in partition_info.replica_executions:
                    node = "no_host"
                    progress = replica_info.progress
                    if send_host != "":
                        send_host += ","
                    if replica_info.node != "":
                        node = replica_info.node
                    send_host += node + "(" + str(progress) + "/100)"
                row = [partition_id, send_host, finish]
                rows.append(row)

            table = tabulate(rows,
                             headers=headers,
                             tablefmt="plain",
                             numalign='left')
            print(table, file=output)
        else:
            print('There are no running tasks', file=output)

    @register_printer
    def query_table(result: dict,
                    with_tasks=True,
                    with_partitions=True,
                    resolve=False,
                    group_by_hub=False,
                    with_table_brief=True,
                    with_statistics_info=False):
        output = io.StringIO()
        if with_table_brief:
            print('table_brief', file=output)
            print(Printer.SPLITTER_SMALL, file=output)
            Printer.pretty_print_struct(result['table'], output=output)
        if with_tasks:
            print(Printer.SPLITTER_LARGE, file=output)
            Printer.pretty_print_struct_list(result, 'tasks', output=output)
        if with_partitions:
            if with_table_brief or with_tasks:
                print(Printer.SPLITTER_LARGE, file=output)
                print("partitions", file=output)
                print(Printer.SPLITTER_SMALL, file=output)
            if group_by_hub:
                Printer.print_partition_by_hubs(result['partitions'],
                                                resolve=resolve,
                                                output=output)
            else:
                Printer.print_partition_by_roles(
                    result['partitions'],
                    resolve=resolve,
                    with_statistics_info=with_statistics_info,
                    output=output)
        return output.getvalue()

    @register_printer
    def query_partition(result: dict,
                        from_partition: int = 0,
                        to_partition: int = 0,
                        resolve=False,
                        group_by_hub=False,
                        with_statistics_info=False):
        output = io.StringIO()
        print(Printer.SPLITTER_LARGE, file=output)
        print("partitions count:", result['table_parts_count'], file=output)
        print(Printer.SPLITTER_SMALL, file=output)
        if group_by_hub:
            Printer.print_partition_by_hubs(result['partitions'],
                                            resolve=resolve,
                                            from_partition=from_partition,
                                            to_partition=to_partition,
                                            output=output)
        else:
            Printer.print_partition_by_roles(
                result['partitions'],
                resolve=resolve,
                with_statistics_info=with_statistics_info,
                from_partition=from_partition,
                to_partition=to_partition,
                output=output)
        return output.getvalue()

    @register_printer
    def query_task(result: dict):
        output = io.StringIO()
        print('task_brief', file=output)
        print(Printer.SPLITTER_SMALL, file=output)
        Printer.pretty_print_struct(result['task'], output=output)
        print(Printer.SPLITTER_LARGE, file=output)
        print('recent executions', file=output)
        print(Printer.SPLITTER_SMALL, file=output)
        Printer.print_recent_executions(result['infos'], output=output)
        return output.getvalue()

    @register_printer
    def query_task_current_execution(result: dict):
        output = io.StringIO()
        print('partition current execution', file=output)
        print(Printer.SPLITTER_SMALL, file=output)
        Printer.print_task_current_executions(result['executions'],
                                              output=output)
        return output.getvalue()

    @register_printer
    def admin_node(result: dict):
        output = io.StringIO()
        Printer.pretty_print_struct_list(result, "node_results", output=output)
        return output.getvalue()

    @register_printer
    def update_node_weight(result: dict):
        output = io.StringIO()
        Printer.pretty_print_struct_list(result, 'node_results', output=output)
        return output.getvalue()

    @register_printer
    def list_nodes(result: dict):
        output = io.StringIO()
        nodes = result['nodes']
        if nodes is None or len(nodes) == 0:
            print("nodes: []", file=output)
            return output.getvalue()

        headers = [
            "node", "id", "index", "op", "is_alive", "hub", "primaries",
            "secondaries", "learners", "total", "estimated", "weight", "score"
        ]
        rows = []
        for node in result['nodes']:
            node_brief = admin_pb.NodeBrief()
            json_format.ParseDict(node, node_brief, ignore_unknown_fields=True)
            rpc_node = node_brief.node
            addr = "{}:{}".format(rpc_node.node_name, rpc_node.port)
            total = node_brief.primary_count + \
                node_brief.secondary_count + node_brief.learner_count
            rows.append([
                addr, node_brief.node_uniq_id,
                utils.index(node_brief.node_index),
                common_pb.AdminNodeOp.Name(node_brief.op),
                utils.bool2str(node_brief.is_alive), node_brief.hub_name,
                node_brief.primary_count, node_brief.secondary_count,
                node_brief.learner_count, total, node_brief.estimated_count,
                node_brief.weight, node_brief.score
            ])
        rows.sort(key=lambda r: r[5] + r[2] + r[0])

        if result['static_indexed']:
            rows = utils.process_rows(rows)

        table = tabulate(rows,
                         headers=headers,
                         tablefmt='plain',
                         numalign='left')
        print(table, file=output)
        return output.getvalue()

    def print_replicas(data: dict, output: io.IOBase = sys.stdout):
        replicas = data['replicas']
        if replicas is None or len(data['replicas']) == 0:
            print("replicas: []", file=output)
            return

        print("replicas", file=output)
        print(Printer.SPLITTER_SMALL, file=output)

        headers = ['table_name', 'partition_id', 'role']
        rows = []
        for replica in data['replicas']:
            replica_info = admin_pb.ReplicaInfo()
            json_format.ParseDict(replica,
                                  replica_info,
                                  ignore_unknown_fields=True)
            rows.append([
                replica_info.table_name, replica_info.partition_id,
                common_pb.ReplicaRole.Name(replica_info.role)
            ])

        table = tabulate(rows,
                         headers=headers,
                         tablefmt='plain',
                         numalign='left')
        print(table, file=output)

    @register_printer
    def query_node_info(result: dict):
        output = io.StringIO()
        print('node_brief', file=output)
        print(Printer.SPLITTER_SMALL, file=output)
        Printer.pretty_print_struct(result['brief'], output=output)
        if 'resource' in result and result['resource'] is not None:
            print(Printer.SPLITTER_SMALL, file=output)
            print('resource', file=output)
            print(Printer.SPLITTER_SMALL, file=output)
            Printer.pretty_print_struct(result['resource'], output=output)
        print(Printer.SPLITTER_LARGE, file=output)
        Printer.print_replicas(result, output=output)
        return output.getvalue()

    @register_printer
    def query_nodes_info(result: dict):
        output = io.StringIO()
        if "nodes" not in result or len(result["nodes"]) == 0:
            print("nodes: []", file=output)
            return output.getvalue()

        nodes = result['nodes']
        for node_info in nodes:
            if node_info['status']['code'] != 0:
                print(node_info['status']['message'], file=output)
            else:
                print('node_brief', file=output)
                print(Printer.SPLITTER_SMALL, file=output)
                Printer.pretty_print_struct(node_info['brief'], output=output)
                if 'resource' in node_info:
                    print(Printer.SPLITTER_SMALL, file=output)
                    print('resource', file=output)
                    print(Printer.SPLITTER_SMALL, file=output)
                    Printer.pretty_print_struct(node_info['resource'],
                                                output=output)
                print(Printer.SPLITTER_SMALL, file=output)
                Printer.print_replicas(node_info, output=output)
            print(Printer.SPLITTER_LARGE, file=output)

        return output.getvalue()

    @register_printer
    def shrink_az(result: dict):
        output = io.StringIO()
        shrinked = result['shrinked']
        if shrinked == None or len(shrinked) == 0:
            print("shrinked: []", file=output)
            return output.getvalue()
        Printer.print_columns(result, 'shrinked', output=output)
        return output.getvalue()

    def expand_json(result):
        return pprint.pformat(result)

    def handle_succeed(name: str, result: dict, **opts):
        if len(result) == 1:
            return "ok"

        del result['status']
        if name not in custom_printers:
            return Printer.expand_json(result)
        else:
            return custom_printers[name](result, **opts)

    def Handle(self, name: str, result: dict, **opts):
        if "status" not in result:
            raise Exception(
                "can't find \"status\" field in response: {}".format(result))

        status = result['status']
        if status['code'] != 0:
            return status['message']
        else:
            return Printer.handle_succeed(name, result, **opts)
