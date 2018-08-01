#!/bin/bash

# To install
# Paste the below lines into /etc/rc.local
# Make sure rc.local uses /bin/bash in its #! 
# (and not /bin/sh, which doesn't support pushd)

pushd /home/pi/EMon-Pi
python3 hardware/program.py &
popd