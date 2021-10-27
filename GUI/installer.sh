#!/bin/bash
# install python3 
sudo apt-get install python3.8 python3-dev python3-pip 
# upgrade pip3
sudo -H pip3 install --upgrade pip

# install requirements
sudo pip install -r requirements.txt

# run app
sudo python3 cvt_gui.py