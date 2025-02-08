#!/usr/bin/env python3

import os

os.environ["DISABLE_NEWBIE_TIPS"] = "TRUE"

import requests
import fire

import teams.reco_arch.colossusdb.proto.mock_onebox_admin_pb2 as mock_admin_pb
import google.protobuf.json_format as pb_json


class MockClient:

    def __init__(self, url: str, service_name: str):
        self.url = url
        self.service_name = service_name

    def start_node(self, node_id: str, port: int, paz: str, node_index: str):
        method = "start_node"
        req = mock_admin_pb.StartNodeRequest()
        req.service_name = self.service_name
        req.node_id = node_id
        req.port = port
        req.paz = paz
        req.node_index = node_index

        data = pb_json.MessageToDict(req,
                                     preserving_proto_field_name=True,
                                     use_integers_for_enums=True)

        req_url = "{}/mock/{}".format(self.url, method)
        resp = requests.post(req_url, json=data)
        resp.raise_for_status()
        print(resp.json())

    def stop_node(self, node_id: str):
        method = "stop_node"
        req = mock_admin_pb.StopNodeRequest()
        req.service_name = self.service_name
        req.node_id = node_id

        data = pb_json.MessageToDict(req,
                                     preserving_proto_field_name=True,
                                     use_integers_for_enums=True)
        req_url = "{}/mock/{}".format(self.url, method)
        resp = requests.post(req_url, json=data)
        resp.raise_for_status()
        print(resp.json())


if __name__ == "__main__":
    fire.Fire(MockClient)
