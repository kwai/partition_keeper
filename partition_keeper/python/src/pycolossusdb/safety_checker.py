#!/usr/bin/env python3

import termcolor
import sys
import json


class SafetyChecker:
    def __init__(self, url: str, service_name: str, skip: bool = False):
        self.url = url
        self.service_name = service_name
        self.skip = skip

    def verify(self, answer, expected):
        if answer == expected:
            termcolor.cprint('correctly answered')
            return
        termcolor.cprint(
            "wrong answer {}, should be {}".format(answer, expected), 'red')
        sys.exit()

    def Check(self, method: str, request_data):
        if self.skip:
            return

        text = "you are going to {} with args:\n{}".format(
            method, json.dumps(request_data, sort_keys=True, indent=4))
        termcolor.cprint(text, 'green')
        termcolor.cprint("please answer the following questions to continue",
                         'green')

        termcolor.cprint('please input the url of partition keeper:', 'cyan')
        self.verify(input(), self.url)

        termcolor.cprint('please input the service_name to be operated',
                         'cyan')
        self.verify(input(), self.service_name)
