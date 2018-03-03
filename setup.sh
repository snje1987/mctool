#!/bin/bash

if [ "$1x" = "-ux" ]
then
    rm -rf py3env
else
    python3 -m venv py3env
    source py3env/bin/activate
    pip install nbt
    deactivate
fi
