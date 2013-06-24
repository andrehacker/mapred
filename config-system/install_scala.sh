#!/bin/bash

#set -x verbose

SCALA_VERSION=2.10.2
SETUP_PATH=/home/andre/Downloads/scala-${SCALA_VERSION}.tgz
TARGET_PATH=/opt/scala

# Remove previous installations
sudo rm -r $TARGET_PATH

# Install
cd ~
sudo mkdir -p $TARGET_PATH
tar -xzf $SETUP_PATH
sudo mv -f scala-${SCALA_VERSION} $TARGET_PATH

# TODO: Add scala/bin to PATH