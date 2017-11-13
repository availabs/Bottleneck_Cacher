#/bin/bash


echo "Starting Cacher for $2 types in $1" > runlog.txt
python BottleneckCacher.py "$1" --type "$2" 2>&1 | tee -a runlog.txt
