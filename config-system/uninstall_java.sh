#!/bin/bash

JAVA_VERSION=38    # version to uninstall. VMs have 38 installed, desktop 43

# Uninstalls Java
# Required for Update
# See http://wiki.ubuntuusers.de/Java/Installation/Oracle_Java?redirect=no#Java-6-JDK

# remove alternatives
sudo update-alternatives --remove "java" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/java"
sudo update-alternatives --remove "javac" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/javac"
sudo update-alternatives --remove "javaws" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/javaws"
sudo update-alternatives --remove "jar" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/jar"
sudo update-alternatives --remove "jps" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/bin/jps"

# Remove browser plugin
sudo update-alternatives --remove "mozilla-javaplugin.so" "/opt/Oracle_Java/jdk1.6.0_${JAVA_VERSION}/jre/lib/amd64/libnpjp2.so"

# Delete folder
sudo rm -rf /opt/Oracle_Java
