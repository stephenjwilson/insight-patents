#!/usr/bin/env bash

# Clone the repository # TODO: make this git clone if not exists, else git pull
git clone https://github.com/stephenjwilson/insight-patents.git

# Install miniconda
if ! which conda > /dev/null; then
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda
    export PATH="$HOME/miniconda/bin:$PATH"
    echo 'export PATH="$HOME/miniconda/bin:$PATH"' >> $HOME/.bashrc
fi

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
# Ensure AWS CLI is installed
pip install awscli --upgrade --user

# Configure aws # TODO: figure out how to automatically do this
# I manually do aws configure right now

# Run
python ./src/python/download.py 1976 2018