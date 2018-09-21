#!/usr/bin/env bash

set -e # stops the execution of the script if there is an error

if [ "$(id -u)" != "0" ]; then
   echo "This script must be run as root" >&2
   exit 1
fi

# Get essential build tools
sudo apt-get install -y build-essential

# Developer Tools
sudo apt-get install -y ssh

# Spark installation

sudo groupadd -g 2000 spark
sudo useradd -m -u 2000 -g 2000 -c 'Apache Spark' -s /bin/bash -d /home/ubuntu/spark spark
# un-tar
tar -zxf /tmp/spark-2.3.1-bin-custom-spark.tgz -C /home/ubuntu/
mv spark-2.3.1-bin-custom-spark spark
echo -e "export SPARK_HOME=/home/ubuntu/spark" | cat >> ~/.profile
echo -e "export PATH=\$PATH:\$SPARK_HOME/bin" | cat >> ~/.profile
echo -e 'export PYSPARK_PYTHON=python3' | cat >> ~/.profile
echo -e 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' | cat >> ~/.profile


# Install Open JDK 8 and JRE 8
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get update -y
sudo apt-get install -y openjdk-8-jdk
sudo apt-get install -y openjdk-8-jre

# Install Oracle JDK and JRE
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update -y
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
sudo apt-get install -y oracle-java8-installer

# Automatically set up the Java 8 environment variables
sudo apt-get install -y oracle-java8-set-default

# Languages
sudo apt-get -y install scala
sudo apt-get -y install python3 python3-dev python3-pip ipython3 python3-nose python3-tornado python3-setuptools

# get sbt repository
wget https://dl.bintray.com/sbt/debian/sbt-0.13.7.deb -P ~/Downloads
sudo dpkg -i ~/Downloads/sbt-*

# install maven
sudo apt-get install -y maven

# Performance Tools
sudo apt-get install -y dstat iotop strace sysstat htop

## PySpark and MLlib dependencies.
#sudo apt-get install -y python-matplotlib python-tornado
#
## SparkR dependencies.
#sudo apt-get install -y r-base
#
# Create /usr/bin/realpath which is used by R to find Java installations
#sudo apt-get install -y realpath

# Root ssh config
sudo sed -i 's/PermitRootLogin.*/PermitRootLogin without-password/g' /etc/ssh/sshd_config
sudo sed -i 's/disable_root.*/disable_root: 0/g' /etc/cloud/cloud.cfg

# Download Github
git clone https://github.com/stephenjwilson/insight-patents.git
# Install python dependencies
pip3 install -r ~/insight-patents/requirements.txt