#!/usr/bin/env python3

import typing
import requests

import teams.reco_arch.colossusdb.proto.partition_service_pb2 as ps_pb
import teams.reco_arch.colossusdb.proto.common_pb2 as common_pb

from colorama import Fore, Style


def parse_cmd_comma_sep_list(input) -> typing.List[str]:
    if type(input) == str:
        return input.split(",")
    elif type(input) == tuple:
        return list(input)
    else:
        raise Exception("unknown hubs {} from cmd input".format(input))


def parse_hubs(hubs) -> typing.List[common_pb.ReplicaHub]:
    if len(hubs) == 0:
        return []
    output = []
    for name_az in parse_cmd_comma_sep_list(hubs):
        name, az = tuple(name_az.split("=", 1))
        h = common_pb.ReplicaHub()
        h.name = name
        h.az = az
        output.append(h)
    return output


def bool2str(b: bool) -> str:
    return b and "true" or "false"


def parse_hostport(hp: str) -> ps_pb.RpcNode:
    host, port = tuple(hp.rsplit(":", 1))
    ans = ps_pb.RpcNode()
    ans.node_name = host
    ans.port = int(port)
    return ans


# s is a string with format "a=b,c=d,e=f"
def str2map(s: str) -> dict:
    result = {}
    if s == "":
        return result
    for parameter in s.split(","):
        key, value = tuple(parameter.split("=", 2))
        result[key] = value

    return result


def index(s: str) -> str:
    if len(s) == 0:
        return "-"
    tokens = s.split(".")
    if len(tokens) == 2:
        return "[%s.%s]" % (tokens[0].zfill(2), tokens[1].zfill(3))
    else:
        return "[" + s + "]"


def process_rows(rows):
    processed_rows = []

    for i in range(len(rows) - 1):
        processed_rows.append(rows[i])
        _, start = rows[i][2][1:-1].split('.')
        _, end = rows[i + 1][2][1:-1].split('.')
        if rows[i][5] == rows[i + 1][5] or (rows[i][5] != rows[i + 1][5]
                                            and int(end) != 0):
            for index_ in range(
                    int(start) + 1 if rows[i][5] == rows[i + 1][5] else 0,
                    int(end)):
                new_index = index("{}.{}".format(str(_), str(index_)))
                processed_rows.append([
                    f"{Fore.RED}{new_index}{Style.RESET_ALL}"
                    if k == 2 else f"{Fore.RED}missing{Style.RESET_ALL}"
                    for k in range(len(rows[i]))
                ])

    if len(rows) > 0:
        processed_rows.append(rows[-1])

    return processed_rows
