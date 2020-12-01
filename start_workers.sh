#!/bin/bash
set -euxo pipefail

for i in {1..3}
do
    python3 ./worker.py $((4000+i)) $i &
done

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

while read -r -n1 key
do
if [[ $key == $'\e' ]];
then
break;
fi
done

