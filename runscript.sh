#/bin/bash

echo "Starting Cacher for Passenger travel times" > runlog.txt
python BottleneckCacher.py ny --type pass 2>&1 | tee -a runlog.txt


echo "Starting Cacher for Truck travel times" >> runlog.txt
python BottleneckCacher.py ny --type trucks 2>&1 | tee -a runlog.txt
