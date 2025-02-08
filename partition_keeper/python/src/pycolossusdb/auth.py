#!/usr/bin/env python3

import fire
import grpc
import teams.reco_arch.colossusdb.proto.partition_service_pb2 as ps_pb
import teams.reco_arch.colossusdb.proto.partition_service_pb2_grpc as grpc_pb


class AuthUpdate:
    def __init__(self):
        pass

    def update_auth(self, addr_file, auth_key : str):
        req = ps_pb.ChangeAuthenticationRequest()
        req.auth_key = auth_key
        with open(addr_file, "r") as f:
            for addr in f:
                channel = grpc.insecure_channel(addr)
                stub = grpc_pb.PartitionServiceStub(channel)
                resp = stub.ChangeAuthentication(req)
                if resp.code != 0:
                    print("change auth addr {} failed, code {}, message {}".format(addr, resp.code, resp.message))

if __name__ == "__main__":
    fire.Fire(AuthUpdate)
