#!/bin/bash
set -euxo pipefail

for i in {0..2}
do
    python ./worker.py $((4000+i)) $i &
done

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

while read -r -n1 key
do
if [[ $key == $'\e' ]];
then
break;
fi
done

