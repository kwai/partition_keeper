#!/bin/bash

ps aux | grep -v grep | grep tables_mgr | awk '{print $2}' | xargs -I {} kill -9 {}
sleep 30
