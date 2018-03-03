#!/bin/bash

source py3env/bin/activate
python app.py "$@"
deactivate
