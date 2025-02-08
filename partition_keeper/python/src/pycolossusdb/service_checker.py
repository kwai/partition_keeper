#!/usr/bin/env python3

import time
import utils

import teams.reco_arch.colossusdb.proto.partition_keeper_admin_pb2 as admin_pb


class ServiceChecker:
    @staticmethod
    def get_checker(service_type):
        return ServiceChecker

    def delete_table_allowed(ksn_name: str, table_name: str):
        return True
