#!/bin/bash

# Copyright 2015 Insight Data Science
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export CLUSTER_NAME=spark-cluster

peg up ./vars/spark_cluster/workers.yml &
peg up ./vars/spark_cluster/master.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} environment

# install
peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt-get install bc"

peg install ${CLUSTER_NAME} hadoop
peg service ${CLUSTER_NAME} hadoop start
peg install ${CLUSTER_NAME} spark
peg service ${CLUSTER_NAME} spark start

# Transfer ENV
peg scp to-rem ${CLUSTER_NAME} 1 .env /home/ubuntu
peg scp to-rem ${CLUSTER_NAME} 2 .env /home/ubuntu
peg scp to-rem ${CLUSTER_NAME} 3 .env /home/ubuntu
peg scp to-rem ${CLUSTER_NAME} 4 .env /home/ubuntu
peg scp to-rem ${CLUSTER_NAME} 5 .env /home/ubuntu

# git clone project
peg sshcmd-cluster ${CLUSTER_NAME} "git clone https://github.com/stephenjwilson/insight-patents.git"

# set-up python env
peg sshcmd-cluster ${CLUSTER_NAME} "cd insight-patents; bash ./src/bash/conda_setup.sh"