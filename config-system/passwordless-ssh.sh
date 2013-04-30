#!/bin/bash

# from http://hortonworks.com/kb/generating-ssh-keys-for-passwordless-login/

# generate the public private keys
ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa

#authorize the key by adding it to the list of authorized keys
cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
