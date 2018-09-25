#!/usr/bin/env bash
# Install conda if needed
#if ! which conda > /dev/null; then
#    wget https://repo.anaconda.com/archive/Anaconda3-5.2.0-Linux-x86_64.sh
#    bash Anaconda3-5.2.0-Linux-x86_64.sh -b
#    echo "export PATH=/home/ubuntu/anaconda3/bin:\$PATH" >>  /home/ubuntu/.bashrc
#    export PATH="$HOME/anaconda3/envs/bin:$PATH"
#    export PYSPARK_PYTHON=$HOME/anaconda3/bin
#fi
# Create environment
#/home/ubuntu/anaconda3/bin/conda create --name insight_patents -y
# Install requirement
#while read requirement; do /home/ubuntu/anaconda3/bin/conda install -n insight_patents --yes $requirement; done < /home/ubuntu/insight-patents/requirements.txt

# Account for special channel. TODO: make cleaner
#/home/ubuntu/anaconda3/bin/conda install -n insight_patents -c conda-forge neo4j-python-driver

#source activate insight_patents
sudo apt-get -y install python3 python3-dev python3-pip ipython3 python3-nose python3-tornado python3-setuptools

pip3 install -r ~/insight-patents/requirements.txt

echo -e 'export PYTHONPATH=${PYTHONPATH}:/home/ubuntu/insight-patents' | cat >> ~/.profile
echo -e 'export PYSPARK_PYTHON=python3' | cat >> ~/.profile