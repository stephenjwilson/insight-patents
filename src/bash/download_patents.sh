#!/usr/bin/env bash

# Clone the repository
git clone https://github.com/stephenjwilson/insight-patents.git

# Install miniconda
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda
export PATH="/home/ubuntu/miniconda/bin:$PATH"

cd insight-patents
# Run the download
# Set-Up Env
ENV_NAME='Insight-Patents'

ENVS=$(conda env list | awk '{print $1}' )
if [[ $ENVS = *$ENV_NAME* ]]; then
   source activate $ENV_NAME
else
    # make virtual environment
    conda create --no-deps -n $ENV_NAME --yes
    source activate $ENV_NAME
    pip install -r requirements.txt
fi;

# Run
python ./src/python/download.py 1976 2018