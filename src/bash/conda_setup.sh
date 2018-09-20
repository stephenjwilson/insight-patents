#!/usr/bin/env bash

wget https://repo.anaconda.com/archive/Anaconda3-5.2.0-Linux-x86_64.sh
bash https://repo.anaconda.com/archive/Anaconda3-5.2.0-Linux-x86_64.sh
conda env create -n insight-patent -f=/home/ubuntu/insight-patents/requirements.txt
source .bashrc
