#!/bin/bash

cd ~/dev

TARGETHOST=192.168.178.200

scp -r stratosphere andre@${TARGETHOST}:~/dev
scp -r hadoop andre@${TARGETHOST}:~/dev
scp -r hive andre@${TARGETHOST}:~/dev
scp -r pig andre@${TARGETHOST}:~/dev
