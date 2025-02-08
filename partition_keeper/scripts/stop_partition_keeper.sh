#!/bin/bash

ps aux | grep -v grep | grep partition_keeper | awk '{print $2}' | xargs -I {} kill -9 {}
sleep 30
