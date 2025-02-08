#!/usr/bin/env python3

import os
os.environ["DISABLE_NEWBIE_TIPS"] = "TRUE"

import requests
import typing
import json
import fire
import utils
import git_revision
import sys
import requests
import service_checker

import teams.reco_arch.colossusdb.proto.common_pb2 as common_pb
import teams.reco_arch.colossusdb.proto.partition_keeper_admin_pb2 as admin_pb
import teams.reco_arch.colossusdb.proto.partition_service_pb2 as ps_pb
import google.protobuf.json_format as pb_json

from output_handler import OutputHandler
from printer import Printer
from safety_checker import SafetyChecker


class HttpClient:
    def __init__(self, url: str, safety_checker: SafetyChecker):
        self.url = url
        self.checker = safety_checker

    def post_to_server(self, method: str, data: dict) -> dict:
        self.checker.Check(method, data)
        resp = requests.post("{}/v1/{}".format(self.url, method), json=data)
        resp.raise_for_status()
        return resp.json()

    def get_from_server(self, method: str, args: typing.Dict[str,
                                                             str]) -> dict:
        resp = None
        if len(args) > 0:
            arg_list = ["{}={}".format(k, v) for k, v in args.items()]
            arg_str = "&".join(arg_list)
            resp = requests.get("{}/v1/{}?{}".format(self.url, method,
                                                     arg_str))
        else:
            resp = requests.get("{}/v1/{}".format(self.url, method))
        resp.raise_for_status()
        return resp.json()


class AdminClient:
    """
    admin client

    service_name must be specified except for the `list_services` command

    you should specify a url for the location of partition keeper.
    """
    @staticmethod
    def _parse_url(url: str) -> str:
        if url.startswith("http:") or url.startswith("https:"):
            return url
        else:
            raise Exception(
                "please specify url for partition keeper with --url <url>")

    def __init__(self,
                 service_name: str = "",
                 url: str = "",
                 handler: OutputHandler = Printer(),
                 skip_safe_check: bool = False):
        safety_checker = SafetyChecker(url, service_name, skip_safe_check)
        url = AdminClient._parse_url(url)
        self.handler = handler
        self.service_name = service_name
        self.http_client = HttpClient(url, safety_checker)

    def _check_service_name(self):
        if self.service_name == "":
            raise Exception(
                "please specify service name with --service_name <service_name>"
            )

    def list_services(self):
        method = "list_services"
        ans = self.http_client.get_from_server(method, dict({}))
        return self.handler.Handle(method, ans)

    def list_all_tables(self):
        method = "list_tables"
        ans = self.http_client.get_from_server(method, dict({}))
        return self.handler.Handle(method, ans)

    def create_service(self, hubs: str, service_type: str,
                       failure_domain_type: str, static_indexed: bool,
                       use_paz: bool):
        """
        Create a service.

        "hubs" should be string with format "yz1=YZ,yz2=YZ"

        "service_type" should be colossusdb_embedding_server, colossusdb_rodis, and so on, which
        must be one of the enum names defined in partition_keeper_admin.proto:ServiceType

        "failure_domain_type" should be PROCESS, HOST or RACK, which restricts the hub allocation
        behavior of a node. For example, if two nodes share same ip address and the failure_domain_type is
        HOST, then the two nodes won't be allocated to different hubs. Generally speaking, an online service should use
        HOST, whereas a cloud deployed service or a local test service should use PROCESS.
        """
        self._check_service_name()
        method = "create_service"
        req = admin_pb.CreateServiceRequest()
        req.service_name = self.service_name
        for hub in utils.parse_hubs(hubs):
            req.nodes_hubs.add().CopyFrom(hub)
        req.service_type = admin_pb.ServiceType.Value(service_type)
        req.failure_domain_type = admin_pb.NodeFailureDomainType.Value(
            failure_domain_type)
        req.static_indexed = static_indexed
        req.use_paz = use_paz

        data = pb_json.MessageToDict(req,
                                     preserving_proto_field_name=True,
                                     use_integers_for_enums=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def delete_service(self):
        self._check_service_name()
        method = "delete_service"
        req = admin_pb.DeleteServiceRequest()
        req.service_name = self.service_name

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def query_service(self):
        self._check_service_name()
        method = "query_service"
        ans = self.http_client.get_from_server(
            method, {"service_name": self.service_name})
        return self.handler.Handle(method, ans)

    def add_hubs(self, hubs: str, try_gather: bool):
        """
        Add hubs

        "hubs" should be string with format "yz1=YZ,yz2=YZ"

        If "try_gather" is true, the added hubs will try to gather nodes from other hubs with same az.
        NOTICE: if no hubs exist on this az before this add,
        no nodes will be gathered given the option is true
        """
        self._check_service_name()
        method = "add_hubs"
        req = admin_pb.AddHubsRequest()
        req.service_name = self.service_name
        req.try_gather = try_gather
        for hub in utils.parse_hubs(hubs):
            h = req.nodes_hubs.add()
            h.name = hub.name
            h.az = hub.az

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def remove_hubs(self,
                    hubs,
                    try_scatter: bool,
                    remove_empty_hub: bool = True):
        """
        Remove hubs

        "hubs" should be string with format "yz1,yz2"

        If try scatter is true, the nodes on removed hub will scatter to other hubs on same az.
        NOTICE: if no hubs exist on this az after this removal,
        then all nodes will be removed in this cluster given the option is true
        """
        self._check_service_name()
        method = "remove_hubs"
        req = admin_pb.RemoveHubsRequest()
        req.service_name = self.service_name
        req.try_scatter = try_scatter
        for hub in utils.parse_cmd_comma_sep_list(hubs):
            h = req.nodes_hubs.add()
            h.name = hub
            h.az = ""
            if not try_scatter and not remove_empty_hub:
                hub_json = self.http_client.get_from_server(
                    "list_nodes", {
                        'service_name': self.service_name,
                        'hub_name': hub
                    })

                if hub_json['nodes'] != None:
                    raise Exception("hub {} not empty".format(hub))

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def _get_schedule_options(self) -> admin_pb.ScheduleOptions:
        self._check_service_name()
        method = "query_service"
        ans = self.http_client.get_from_server(
            method, {"service_name": self.service_name})

        if ans['status']['code'] != admin_pb.AdminError.kOk:
            raise Exception("query service {} failed. result: {}".format(
                self.service_name, ans['status']['message']))

        if "sched_opts" not in ans:
            raise Exception(
                "can't find sched_opts in service {}, perhaps a staled server")
        sched_opts = admin_pb.ScheduleOptions()
        pb_json.ParseDict(ans['sched_opts'],
                          sched_opts,
                          ignore_unknown_fields=True)
        return sched_opts

    def _update_schedule_options(self, opts: admin_pb.ScheduleOptions,
                                 name: str):
        self._check_service_name()
        method = "update_schedule_options"

        req = admin_pb.UpdateScheduleOptionsRequest()
        req.service_name = self.service_name
        req.updated_option_names.append(name)
        req.sched_opts.CopyFrom(opts)
        data = pb_json.MessageToDict(req,
                                     preserving_proto_field_name=True,
                                     use_integers_for_enums=True,
                                     including_default_value_fields=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def enable_primary_scheduler(self):
        sched_opts = self._get_schedule_options()
        if sched_opts.enable_primary_scheduler:
            return "primary scheduler already enabled"
        sched_opts.enable_primary_scheduler = True
        return self._update_schedule_options(sched_opts,
                                             "enable_primary_scheduler")

    def disable_primary_scheduler(self):
        sched_opts = self._get_schedule_options()
        if not sched_opts.enable_primary_scheduler:
            return "primary scheduler already disabled"
        sched_opts.enable_primary_scheduler = False
        return self._update_schedule_options(sched_opts,
                                             "enable_primary_scheduler")

    def update_schedule_ratio(self, ratio: int):
        """
        update schedule ratio, ratio should be within [1,1000]
        """
        sched_opts = self._get_schedule_options()
        sched_opts.max_sched_ratio = ratio
        return self._update_schedule_options(sched_opts, "max_sched_ratio")

    def update_estimator(self, estimator: str):
        """
        Update estimator

        The service will use the given estimator to score the node and the number
        of replicas on a node will be calculated based on the score.

        Currently supported estimators: default, cpu_cores, disk_cap
        """
        sched_opts = self._get_schedule_options()
        sched_opts.estimator = estimator
        return self._update_schedule_options(sched_opts, "estimator")

    def force_rescore_nodes(self):
        sched_opts = self._get_schedule_options()
        sched_opts.force_rescore_nodes = True
        return self._update_schedule_options(sched_opts, "force_rescore_nodes")

    def enable_split_balancer(self):
        sched_opts = self._get_schedule_options()
        if sched_opts.enable_split_balancer:
            return "split balancer already enabled"
        sched_opts.enable_split_balancer = True
        return self._update_schedule_options(sched_opts,
                                             "enable_split_balancer")

    def disable_split_balancer(self):
        sched_opts = self._get_schedule_options()
        if not sched_opts.enable_split_balancer:
            return "split balancer already disable"
        sched_opts.enable_split_balancer = False
        return self._update_schedule_options(sched_opts,
                                             "enable_split_balancer")

    def cancel_force_rescore_nodes(self):
        sched_opts = self._get_schedule_options()
        sched_opts.force_rescore_nodes = False
        return self._update_schedule_options(sched_opts, "force_rescore_nodes")

    def enable_hash_arranger_add_replica_first(self):
        sched_opts = self._get_schedule_options()
        sched_opts.hash_arranger_add_replica_first = True
        return self._update_schedule_options(
            sched_opts, "hash_arranger_add_replica_first")

    def disable_hash_arranger_add_replica_first(self):
        sched_opts = self._get_schedule_options()
        sched_opts.hash_arranger_add_replica_first = False
        return self._update_schedule_options(
            sched_opts, "hash_arranger_add_replica_first")

    def update_hash_arranger_max_overflow_replicas(self, count):
        sched_opts = self._get_schedule_options()
        sched_opts.hash_arranger_max_overflow_replicas = count
        return self._update_schedule_options(
            sched_opts, "hash_arranger_max_overflow_replicas")

    def update_max_learning_parts_per_node(self, max_learning_parts: int):
        sched_opts = self._get_schedule_options()
        sched_opts.max_learning_parts_per_node = max_learning_parts
        return self._update_schedule_options(sched_opts, "max_learning_parts_per_node")

    def _update_hubs(self, req: admin_pb.UpdateHubsRequest):
        self._check_service_name()
        req.service_name = self.service_name

        method = "update_hubs"
        data = pb_json.MessageToDict(req,
                                     preserving_proto_field_name=True,
                                     use_integers_for_enums=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def disallow_primary_on_hubs(self, hubs):
        """
        Disallow to schedule primaries on given hubs

        "hubs" should be in format of "hub1,hub2...."
        """
        req = admin_pb.UpdateHubsRequest()
        for hub in utils.parse_cmd_comma_sep_list(hubs):
            target_hub = req.nodes_hubs.add()
            target_hub.name = hub
            target_hub.az = ""
            target_hub.disallowed_roles.append(common_pb.ReplicaRole.kPrimary)
        return self._update_hubs(req)

    def allow_all_roles_on_hubs(self, hubs):
        """
        Allow to schedule primaries on given hubs

        "hubs" should be in format of "hub1,hub2...."
        """
        req = admin_pb.UpdateHubsRequest()
        for hub in utils.parse_cmd_comma_sep_list(hubs):
            target_hub = req.nodes_hubs.add()
            target_hub.name = hub
        return self._update_hubs(req)

    def give_hints(self, hints: str):
        """
        give hints, currently only support giving hints on which hub to allocate node.
        and these hints are one shot, which means that hints will be cleaned once a new started
        server finish an allocation of hub.

        hints should be format "ip:port=hub1,ip:port2=hub..." or "ip=hub1,ip=hub2"
        """
        self._check_service_name()
        method = "give_hints"
        req = admin_pb.GiveHintsRequest()
        req.service_name = self.service_name
        req.match_port = True
        for hint in hints.split(","):
            instance, hub = tuple(hint.split("=", 2))
            node_hint = req.hints[instance]
            node_hint.hub = hub

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def recall_hints(self, hints: str):
        self._check_service_name()
        method = "recall_hints"

        req = admin_pb.RecallHintsRequest()
        req.service_name = self.service_name
        for item in map(utils.parse_hostport, hints.split(",")):
            req.nodes.append(item)

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def create_table(self,
                     table_name: str,
                     partition_count: int,
                     base_table: str,
                     json_arg_file: str = "",
                     json_args: str = "",
                     kconf_path: str = "",
                     restore_path: str = "",
                     traffic_kconf_path: str = ""):
        """
        create a table

        you can specify one of "json_arg_file" or "json_args" as the table json args:
        arg_json_file is the file path for a json file.

        args is a string with format "a=b,c=d,e=f", which will be translated to a simple json map
        where keys and values are string item, that is to say: {"a":"b", "c":"d", "e":"f"}.

        if both arg_json_file & args are specified, the arg_json_file is preferred.

        if restore_path is not empty, the table will be restored from restore_path
        """
        self._check_service_name()
        method = "create_table"

        req = admin_pb.CreateTableRequest()
        req.service_name = self.service_name
        req.restore_path = restore_path
        req.traffic_kconf_path = traffic_kconf_path
        req.table.table_name = table_name
        req.table.hash_method = "crc32"
        req.table.parts_count = partition_count
        req.table.kconf_path = kconf_path
        req.table.base_table = base_table

        if json_arg_file != "":
            with open(json_arg_file, "r") as f:
                req.table.json_args = f.read()
        elif json_args != "":
            d = {}
            for item in json_args.split(","):
                key, value = tuple(item.split('=', 1))
                d[key] = value
            req.table.json_args = json.dumps(d)

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)

    def delete_table(self,
                     table_name: str,
                     ksn_name: str = "",
                     skip_traffic_check: bool = False,
                     skip_checkpoint_check: bool = False,
                     clean_task_side_effect: bool = False,
                     clean_delay_minutes: int = 0):
        """
        delete table

        If the clean_task_side_effect is true, all data of the task will be deleted after clean_delay_minutes minutes
        clean_delay_minutes must greater than or equal to 0
        """
        self._check_service_name()
        method = "delete_table"

        req = admin_pb.DeleteTableRequest()
        req.service_name = self.service_name
        req.table_name = table_name
        req.clean_task_side_effect = clean_task_side_effect
        req.clean_delay_minutes = clean_delay_minutes

        service_json = self.http_client.get_from_server(
            "query_service", {"service_name": self.service_name})

        checker = service_checker.ServiceChecker.get_checker(
            service_json['service_type'])

        if not skip_traffic_check and not checker.delete_table_allowed(
                ksn_name, table_name):
            raise Exception("{} traffic is not zero".format(table_name))

        task_json = self.http_client.get_from_server(
            "query_task", {
                'service_name': self.service_name,
                'table_name': table_name,
                "task_name": "checkpoint"
            })

        if not skip_checkpoint_check and task_json['task'] == None:
            raise Exception("{} do not have checkpoint".format(table_name))

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)

    def restore_table(self, table_name: str, restore_path: str,
                      max_concurrent_nodes_per_hub: int,
                      max_concurrent_parts_per_node: int):
        """
        restore table

        the table will be restored from restore_path
        """
        self._check_service_name()
        method = "restore_table"

        req = admin_pb.RestoreTableRequest()
        req.service_name = self.service_name
        req.table_name = table_name
        req.restore_path = restore_path

        req.opts.max_concurrent_nodes_per_hub = max_concurrent_nodes_per_hub
        req.opts.max_concurrent_parts_per_node = max_concurrent_parts_per_node

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)

    def set_table_grayscale(self, table_name: str, grayscale: int):
        self._check_service_name()
        method = "update_table"

        req = admin_pb.UpdateTableRequest()
        req.service_name = self.service_name
        req.table.table_name = table_name
        req.table.schedule_grayscale = grayscale

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def update_table_json_args(
        self,
        table_name: str,
        json_arg_file: str = "",
        json_args: str = "",
    ):
        """
        update table json args table

        Rodis can disable routing to zk by setting the value of zk_hosts to null or empty
        """

        self._check_service_name()
        method = "update_table_json_args"

        req = admin_pb.UpdateTableJsonArgsRequest()
        req.service_name = self.service_name
        req.table_name = table_name
        if json_arg_file != "":
            with open(json_arg_file, "r") as f:
                req.json_args = f.read()
        elif json_args != "":
            d = {}
            for item in json_args.split(","):
                key, value = tuple(item.split('=', 1))
                d[key] = value
            req.json_args = json.dumps(d)

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def list_tables(self):
        self._check_service_name()
        method = "list_tables"

        ans = self.http_client.get_from_server(
            method, {'service_name': self.service_name})
        return self.handler.Handle(method, ans)

    def query_table(self,
                    table_name: str,
                    with_tasks: bool = True,
                    with_partitions: bool = True,
                    resolve: bool = False,
                    group_by_hub: bool = False,
                    with_table_brief: bool = True,
                    with_statistics_info: bool = False):
        self._check_service_name()
        method = "query_table"
        ans = self.http_client.get_from_server(
            method, {
                'service_name': self.service_name,
                'table_name': table_name,
                "with_tasks": utils.bool2str(with_tasks),
                "with_partitions": utils.bool2str(with_partitions)
            })

        return self.handler.Handle(
            method,
            ans,
            with_tasks=with_tasks,
            with_partitions=with_partitions,
            resolve=resolve,
            group_by_hub=group_by_hub,
            with_table_brief=with_table_brief,
            with_statistics_info=with_statistics_info,
        )

    def query_partition(self,
                        table_name: str,
                        from_partition: int = 0,
                        to_partition: int = 0,
                        resolve: bool = False,
                        group_by_hub: bool = False,
                        with_statistics_info: bool = False):
        self._check_service_name()
        method = "query_partition"
        ans = self.http_client.get_from_server(
            method, {
                'service_name': self.service_name,
                'table_name': table_name,
                "from_partition": str(from_partition),
                "to_partition": str(to_partition)
            })

        return self.handler.Handle(
            method,
            ans,
            from_partition=from_partition,
            to_partition=to_partition,
            resolve=resolve,
            group_by_hub=group_by_hub,
            with_statistics_info=with_statistics_info,
        )

    def remove_replicas(self,
                        table_name: str,
                        replica: str = "",
                        replicas_file: str = ""):
        """
        Remove replicas manually

        If you want to remove only one replica, you can use --replica=ip:port@<partition_id>.

        If you want to remove lots replicas, you can use --replicas_file=<file_name> to specify
        a replica_file, which is a multi-line file that each line represents a replica to remove with
        the format of "ip:port@<partition_id>"
        """
        def _parse_replica(
            replica_desc: str
        ) -> admin_pb.ManualRemoveReplicasRequest.ReplicaItem:
            output = admin_pb.ManualRemoveReplicasRequest.ReplicaItem()
            node, part = tuple(replica_desc.split("@", 1))
            output.partition_id = int(part)
            output.node.CopyFrom(utils.parse_hostport(node))

            return output

        def _parse_replicas_file(output: admin_pb.ManualRemoveReplicasRequest,
                                 replicas_file: str):
            with open(replicas_file) as f:
                for line in f:
                    output.replicas.append(_parse_replica(line.strip()))

        self._check_service_name()
        method = "remove_replicas"
        req = admin_pb.ManualRemoveReplicasRequest()
        req.service_name = self.service_name
        req.table_name = table_name
        if len(replica) == 0:
            _parse_replicas_file(req, replicas_file)
        else:
            req.replicas.append(_parse_replica(replica))

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def _get_current_task(self, table_name: str,
                          task_name: str) -> admin_pb.PeriodicTask:
        self._check_service_name()
        method = "query_table"
        ans = self.http_client.get_from_server(
            method, {
                'service_name': self.service_name,
                'table_name': table_name,
                'with_tasks': 'true',
                'with_partitions': 'false'
            })

        if ans['status']['code'] != admin_pb.AdminError.kOk:
            raise Exception("query table {} result: {}".format(
                table_name, ans['status']['message']))

        for task in ans['tasks']:
            task_pb = admin_pb.PeriodicTask()
            pb_json.ParseDict(task, task_pb, ignore_unknown_fields=True)
            if task_pb.task_name == task_name:
                return task_pb

        raise Exception("can't find task {} in table {}".format(
            task_name, table_name))

    def create_task(self,
                    table_name: str,
                    task_name: str,
                    first_trigger_unix_secs: int,
                    period_secs: int,
                    keep_nums: int,
                    notify_mode: str,
                    max_concurrency_per_node: int = 0,
                    max_concurrent_nodes: int = 0,
                    max_concurrent_hubs: int = 0,
                    args: str = ""):
        """
        create a task

        task_name must be one of the following:[checkpoint, schema_change]

        first_trigger_unix_secs : means that the first task will be executed on this event

        period_secs : means how often a task is executed

        keep_nums : means to preserve the latest keep_nums tasks

        notify_mode : It could be NOTIFY_LEADER/NOTIFY_EVERY_REGION/NOTIFY_ALL

        max_concurrency_per_node : partition concurrency, means how many partition can be executed on a node at the same time. 0 means unlimited

        max_concurrent_nodes : Node concurrency, means how many nodes can perform tasks at the same time. 0 means unlimited

        max_concurrent_hubs : hub concurrency, means how many hub can perform tasks at the same time. 0 means unlimited

        args is a string with format "a=b,c=d,e=f", which will be translated to a simple json map
        where keys and values are string item, that is to say: {"a":"b", "c":"d", "e":"f"}.

        """
        self._check_service_name()
        method = "create_task"

        # don't use pb_json.MessageToDict here as protobuf will output string if
        # a field is int64
        data = dict()
        data["service_name"] = self.service_name
        data["table_name"] = table_name
        data["task"] = {
            "task_name": task_name,
            "first_trigger_unix_seconds": first_trigger_unix_secs,
            "period_seconds": period_secs,
            "max_concurrency_per_node": max_concurrency_per_node,
            "max_concurrent_nodes": max_concurrent_nodes,
            "max_concurrent_hubs": max_concurrent_hubs,
            "notify_mode": admin_pb.TaskNotifyMode.Value(notify_mode),
            "keep_nums": keep_nums,
        }
        data["task"]["args"] = utils.str2map(args)

        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def delete_task(self,
                    table_name: str,
                    task_name: str,
                    clean_task_side_effect: bool = False,
                    clean_delay_minutes: int = 0):
        """
        delete task

        If the clean_task_side_effect is true, all side effect of the task will be deleted after clean_delay_minutes minutes
        clean_delay_minutes must greater than or equal to 0
        """
        self._check_service_name()
        method = "delete_task"
        req = admin_pb.DeleteTaskRequest()
        req.service_name = self.service_name
        req.table_name = table_name
        req.task_name = task_name
        req.clean_task_side_effect = clean_task_side_effect
        req.clean_delay_minutes = clean_delay_minutes
        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def update_task(self,
                    table_name: str,
                    task_name: str,
                    first_trigger_unix_secs: int = -1,
                    period_secs: int = -1,
                    notify_mode: str = "",
                    keep_nums: int = -2,
                    max_concurrent_nodes: int = -1,
                    max_concurrent_hubs: int = -1,
                    max_concurrency_per_node: int = -1,
                    paused: str = "",
                    args: str = ""):
        """
        update task's options, including: first_trigger_unix_secs, period_secs, notify_mode,
        keep_nums(-1 means keep all),
        max_concurrent_nodes(0 means unlimited), max_concurrent_hubs(0 means unlimited), max_concurrency_per_node(0 means unlimited)
        paused, args.

        notify_mode : It could be NOTIFY_LEADER/NOTIFY_EVERY_REGION/NOTIFY_ALL

        args is a string with format "a=b,c=d,e=f", which will be translated to a simple json map
        where keys and values are string item, that is to say: {"a":"b", "c":"d", "e":"f"}.
        """
        self._check_service_name()
        method = "update_task"

        current_task = self._get_current_task(table_name, task_name)
        if first_trigger_unix_secs == -1:
            first_trigger_unix_secs = current_task.first_trigger_unix_seconds
        if period_secs == -1:
            period_secs = current_task.period_seconds
        if notify_mode == "":
            notify_mode = admin_pb.TaskNotifyMode.Name(
                current_task.notify_mode)
        if keep_nums == -2:
            keep_nums = current_task.keep_nums
        if max_concurrent_nodes == -1:
            max_concurrent_nodes = current_task.max_concurrent_nodes
        if max_concurrent_hubs == -1:
            max_concurrent_hubs = current_task.max_concurrent_hubs
        if max_concurrency_per_node == -1:
            max_concurrency_per_node = current_task.max_concurrency_per_node
        paused_flag = current_task.paused
        if paused != "":
            if paused != "true" and paused != "false":
                raise Exception(
                    "paused must be \"true\" or \"false\" or just kept empty, but \"{}\" provided"
                    .format(paused))
            paused_flag = (paused == "true")

        # don't use pb_json.MessageToDict here as protobuf will output string if
        # a field is int64
        data = dict()
        data["service_name"] = self.service_name
        data["table_name"] = table_name
        data["task"] = {
            "task_name": task_name,
            "first_trigger_unix_seconds": first_trigger_unix_secs,
            "period_seconds": period_secs,
            "notify_mode": admin_pb.TaskNotifyMode.Value(notify_mode),
            "max_concurrent_nodes": max_concurrent_nodes,
            "max_concurrent_hubs": max_concurrent_hubs,
            "max_concurrency_per_node": max_concurrency_per_node,
            "keep_nums": keep_nums,
            "paused": paused_flag,
        }

        if args != "":
            data["task"]["args"] = utils.str2map(args)
        else:
            data["task"]["args"] = dict(current_task.args)

        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def query_task(self, table_name: str, task_name: str):
        self._check_service_name()
        method = "query_task"
        ans = self.http_client.get_from_server(
            method, {
                'service_name': self.service_name,
                'table_name': table_name,
                "task_name": task_name
            })

        return self.handler.Handle(method, ans)

    def query_task_current_execution(self, table_name: str, task_name: str):
        self._check_service_name()
        method = "query_task_current_execution"
        ans = self.http_client.get_from_server(
            method, {
                'service_name': self.service_name,
                'table_name': table_name,
                "task_name": task_name
            })
        return self.handler.Handle(method, ans)

    def delete_task_side_effect(self, table_name: str, task_name: str):
        """
        trigger delete task side effect now
        """
        self._check_service_name()
        method = "trigger_delete_task_side_effect"
        ans = self.http_client.post_to_server(
            method, {
                'service_name': self.service_name,
                'table_name': table_name,
                "task_name": task_name
            })
        return self.handler.Handle(method, ans)

    def config_checkpoint(self,
                          table_name: str,
                          first_trigger_unix_secs: int = -1,
                          period_secs: int = -1,
                          notify_mode: str = ""):
        """
        config checkpoint

        first_trigger_unix_secs must larger than 0
        period_secs mst larger than 0
        """
        return self.update_task(table_name, "checkpoint",
                                first_trigger_unix_secs, period_secs,
                                notify_mode)

    def trigger_checkpoint(self, table_name: str):
        """
        trigger checkpoint now
        """
        return self.update_task(table_name,
                                "checkpoint",
                                first_trigger_unix_secs=0,
                                period_secs=0)

    def list_nodes(self,
                   az: str = "",
                   hub_name: str = "",
                   table_name: str = ""):
        """
        list nodes for a service

        if az is specified, only nodes on this az will be displayed REGARDLESS OF what hub_name is.

        if az isn't specified and hub_name is specified, then only nodes on this hub will be displayed.
        """
        self._check_service_name()
        method = "list_nodes"
        params = {'service_name': self.service_name}

        if az != "":
            params['az'] = az
        elif hub_name != "":
            params['hub_name'] = hub_name

        if table_name != "":
            params['table_name'] = table_name

        query_service_json = self.http_client.get_from_server(
            "query_service", {"service_name": self.service_name})

        ans = self.http_client.get_from_server(method, params)

        ans['static_indexed'] = query_service_json['static_indexed']
        return self.handler.Handle(method, ans)

    def query_node_info(self,
                        host_port: str,
                        table_name: str = "",
                        only_brief: bool = False,
                        match_port: bool = True):
        self._check_service_name()
        method = "query_node_info"
        node = utils.parse_hostport(host_port)
        ans = self.http_client.get_from_server(
            method, {
                'service_name': self.service_name,
                'node_name': node.node_name,
                'port': str(node.port),
                'table_name': table_name,
                "only_brief": utils.bool2str(only_brief),
                "match_port": utils.bool2str(match_port),
            })
        return self.handler.Handle(method, ans)

    def query_nodes_info(self,
                         host_ports: str,
                         table_name: str = "",
                         only_brief: bool = False,
                         match_port: bool = True):
        """
        query nodes info

        host_ports are host:port separated by ",", like: "127.0.0.1:111,127.0.0.1:112"
        """
        self._check_service_name()
        self.http_client.checker.skip = True
        method = "query_nodes_info"

        req = admin_pb.QueryNodesInfoRequest()
        req.only_brief = only_brief
        req.match_port = match_port
        req.service_name = self.service_name
        if table_name != "":
            req.table_name = table_name

        items = map(utils.parse_hostport, host_ports.split(","))
        for n in items:
            node = req.nodes.add()
            node.node_name = n.node_name
            node.port = n.port

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def _admin_node(self, nodes: typing.List[ps_pb.RpcNode],
                    op: common_pb.AdminNodeOp):
        self._check_service_name()
        method = "admin_node"
        req = admin_pb.AdminNodeRequest()
        req.service_name = self.service_name
        req.match_port = True
        for n in nodes:
            node = req.nodes.add()
            node.node_name = n.node_name
            node.port = n.port
        req.op = op

        data = pb_json.MessageToDict(req,
                                     preserving_proto_field_name=True,
                                     use_integers_for_enums=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def restart_nodes(self, host_ports: str):
        """
        restarting nodes

        host_ports are host:port separated by ",", like: "127.0.0.1:111,127.0.0.1:112"
        """
        items = map(utils.parse_hostport, host_ports.split(","))
        return self._admin_node(nodes=items, op=common_pb.kRestart)

    def offline_nodes(self, host_ports: str):
        """
        offline nodes

        host_ports are host:port separated by ",", like: "127.0.0.1:111,127.0.0.1:112"
        """
        items = map(utils.parse_hostport, host_ports.split(","))
        return self._admin_node(nodes=items, op=common_pb.kOffline)

    def cancel_nodes_op(self, host_ports: str):
        """
        cancel nodes op

        host_ports are host:port separated by ",", like: "127.0.0.1:111,127.0.0.1:112"
        """
        items = map(utils.parse_hostport, host_ports.split(","))
        return self._admin_node(nodes=items, op=common_pb.kNoop)

    def update_node_weight(self, new_weights: str):
        """
        update weights

        new_weights should be in format <ip:port>=<weight>,<ip:port>=<weight>
        """
        self._check_service_name()
        method = "update_node_weight"
        update_req = admin_pb.UpdateNodeWeightRequest()
        update_req.service_name = self.service_name
        for item in new_weights.split(","):
            hp, weight = tuple(item.split("=", 1))
            parsed_hp = utils.parse_hostport(hp)
            node = update_req.nodes.add()
            node.node_name = parsed_hp.node_name
            node.port = parsed_hp.port
            update_req.weights.append(float(weight))
        data = pb_json.MessageToDict(update_req,
                                     preserving_proto_field_name=True,
                                     use_integers_for_enums=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def shrink_az(self, az: str, new_size: int):
        """
        shrink az size

        az should be a valid az name which is initialized in creating cluster or add hub

        new_size specifies the new nodes count in this az, should be
        a value larger than 0 and smaller than the current node count of this az. 
        a value non-larger-than-0 will cause an error reported, 
        whereas a value which isn't smaller than current size of az will take no effect.
        """
        self._check_service_name()
        method = "shrink_az"
        req = admin_pb.ShrinkAzRequest()
        req.service_name = self.service_name
        req.az = az
        req.new_size = new_size

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)

    def expand_azs(self, azs_new_size: str):
        """
        expand azs

        azs_new_size should be in format az=<new_size>,<az>=<new_size>, where new_size represents
        the new size of this az, and which must be larger than az's current size

        if this command executes successfully, the scheduler won't work until enough nodes are detected
        for given az, which is very useful for planned scale-out of a service as unnecessary scheduling
        is avoided
        """
        self._check_service_name()
        method = "expand_azs"
        req = admin_pb.ExpandAzsRequest()
        req.service_name = self.service_name
        size_map = utils.str2map(azs_new_size)
        for az, size_str in size_map.items():
            opt = req.az_options.add()
            opt.az = az
            opt.new_size = int(size_str)

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)

    def cancel_expand_azs(self, az_names: str):
        """
        cancel expand azs

        az_names should be in format "<AZ1>,<AZ2>", which means az size watcher for the azs are cancelled
        """
        self._check_service_name()
        method = "cancel_expand_azs"
        req = admin_pb.CancelExpandAzsRequest()
        req.service_name = self.service_name
        req.azs.extend(utils.parse_cmd_comma_sep_list(az_names))

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)

    def split_table(self, table_name: str, new_split_version: int,
                    max_concurrent_parts: int, delay_seconds: int):
        """
        split table 
        """
        self._check_service_name()
        method = "split_table"
        req = admin_pb.SplitTableRequest()
        req.service_name = self.service_name
        req.table_name = table_name
        req.new_split_version = new_split_version
        req.options.max_concurrent_parts = max_concurrent_parts
        req.options.delay_seconds = delay_seconds
        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def replace_nodes(self, replacement: str):
        """
        replace nodes

        replacement should be in format src=target,src=target,
        where src and target are hostname:port

        if this command executes successfully, the scheduler won't work until all targets are
        detected.
        for each detected target, it will be allocated to the same hub with it's src and the src will
        be marked offline automatically
        """
        self._check_service_name()
        method = "replace_nodes"
        req = admin_pb.ReplaceNodesRequest()
        req.service_name = self.service_name
        replace_map = utils.str2map(replacement)
        for src, dst in replace_map.items():
            src_node = req.src_nodes.add()
            n = utils.parse_hostport(src)
            src_node.node_name = n.node_name
            src_node.port = n.port

            dst_node = req.dst_nodes.add()
            n = utils.parse_hostport(dst)
            dst_node.node_name = n.node_name
            dst_node.port = n.port

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)

    def assign_hub(self, node: str, hub: str):
        """
        assign hub for a node

        node should be in format of "ip:port"
        """
        self._check_service_name()
        method = "assign_hub"
        req = admin_pb.AssignHubRequest()
        req.service_name = self.service_name
        req.node.CopyFrom(utils.parse_hostport(node))
        req.hub = hub

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)
        return self.handler.Handle(method, ans)

    def switch_scheduler_status(self, flag: bool):
        self._check_service_name()
        method = "switch_scheduler_status"

        req = admin_pb.SwitchSchedulerStatusRequest()
        req.service_name = self.service_name
        req.enable = flag

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)

    def enable_scheduler(self):
        return self.switch_scheduler_status(flag=True)

    def disable_scheduler(self):
        return self.switch_scheduler_status(flag=False)

    def switch_kess_poller_status(self, flag: bool):
        self._check_service_name()
        method = "switch_kess_poller_status"

        req = admin_pb.SwitchKessPollerStatusRequest()
        req.service_name = self.service_name
        req.enable = flag

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)

    def enable_kess_poller(self):
        return self.switch_kess_poller_status(flag=True)

    def disable_kess_poller(self):
        return self.switch_kess_poller_status(flag=False)

    def remove_watcher(self, watcher_name: str):
        self._check_service_name()
        method = "remove_watcher"

        req = admin_pb.RemoveWatcherRequest()
        req.service_name = self.service_name
        req.watcher_name = watcher_name

        data = pb_json.MessageToDict(req, preserving_proto_field_name=True)
        ans = self.http_client.post_to_server(method, data)

        return self.handler.Handle(method, ans)


if __name__ == "__main__":
    if len(sys.argv) == 2 and (sys.argv[1] == "version"
                               or sys.argv[1] == "--version"):
        git_revision.PrintVersion()
    else:
        fire.Fire(AdminClient)
