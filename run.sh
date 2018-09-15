
#!/usr/bin/env bash

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
python ./src/python/main.py