#!/usr/bin/env bash
# Install conda if needed
if ! which conda > /dev/null; then
    wget https://repo.anaconda.com/archive/Anaconda3-5.2.0-Linux-x86_64.sh
    bash Anaconda3-5.2.0-Linux-x86_64.sh -b
    echo "export PATH=/home/ubuntu/anaconda3/bin:\$PATH" >>  /home/ubuntu/.bashrc
    export PATH="$HOME/anaconda3/bin:$PATH"
fi
# Create environment
/home/ubuntu/anaconda3/bin/conda env create -n insight-patent
# Install requirement
while read requirement; do /home/ubuntu/anaconda3/bin/conda install --yes $requirement; done < /home/ubuntu/insight-patents/requirements.txt

