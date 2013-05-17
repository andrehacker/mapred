#!/bin/bash

# Assumes that files are already copied to the specified folder (probably /opt/eclipse)
INSTALL_PATH=/opt/eclipse/eclipse_4_2_2

# move desktop file
sudo cp eclipse.desktop /usr/share/applications/

# create symlink
cd /usr/local/bin
sudo ln -s ${INSTALL_PATH}/eclipse

