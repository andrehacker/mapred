#!/bin/bash

JAVA_VERSION=43    # VMs have 38 installed
SETUP_PATH=~/Downloads/jdk-6u${JAVA_VERSION}-linux-x64.bin

# assuming that java setup file is downloaded
# For Update, uninstall first
# See http://wiki.ubuntuusers.de/Java/Installation/Oracle_Java?redirect=no#Java-6-JDK

# Install
cd ~
chmod u+x $SETUP_PATH
.${SETUP_PATH}

# Copy and change owner
sudo mkdir -p /opt/Oracle_Java
sudo cp -a jdk1.6.0_${JAVA_VERSION}/ /opt/Oracle_Java/
sudo chown -R root:root /opt/Oracle_Java/* 

# delete temporary folder
rm -r ~/jdk1.6.0_${JAVA_VERSION}

# setup alternatives (links)
sudo update-alternatives --install "/usr/bin/java" "java" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/java" 1
sudo update-alternatives --install "/usr/bin/javac" "javac" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/javac" 1
sudo update-alternatives --install "/usr/bin/javaws" "javaws" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/javaws" 1
sudo update-alternatives --install "/usr/bin/jar" "jar" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/jar" 1 

# setup browser plugin
sudo update-alternatives --install "/usr/lib/mozilla/plugins/mozilla-javaplugin.so" "mozilla-javaplugin.so" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/jre/lib/amd64/libnpjp2.so" 1 

# configure alternatives
sudo update-alternatives --set "java" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/java"
sudo update-alternatives --set "javac" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/javac"
sudo update-alternatives --set "javaws" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/javaws"
sudo update-alternatives --set "jar" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/jar"

#configure browser plugin
sudo update-alternatives --set "mozilla-javaplugin.so" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/jre/lib/amd64/libnpjp2.so"

