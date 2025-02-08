#!/bin/bash

ps aux | grep -v grep | grep sd_server | awk '{print $2}' | xargs -I {} kill -9 {}
sleep 30
