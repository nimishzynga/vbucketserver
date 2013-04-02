#!/bin/bash
. /etc/init.d/functions

vbaArray=()
moxiArray=()
vbaCount=2
moxiCount=2
function ErrandExit {
    sudo pkill python
    exit
}

function startVBA {
echo "starting vba" 
sudo python2.4 vbucket_agent.py -e 127.0.0.$1 > /dev/null 2>&1 &
echo $!
}

function startMoxi {
echo "starting moxi"
sudo python2.4 moxi.py -e 127.0.0.$1 > /dev/null 2>&1 &
echo $!
}

function stopVBA {
    sudo kill -9 $1
}

function stopMoxi {
    sudo kill -9 $1
}

#sudo /sbin/ifconfig lo:$i 127.0.0.$i netmask 255.255.255.0 

[[ ! -f $1 ]] && echo "Not command file" && ErrandExit

echo "ha ha ha"

cat $1 | while read line; do
echo "line is", $line
cmd=($line)
case ${cmd[0]} in
"start")
if [[ ${cmd[1]} == "VBA" ]];then
    for ((i=0;i<${cmd[2]};i++));do
    pid=$(startVBA $vbaCount)
    vbaArray[${cmd[2]}]=pid
    vbaCount=`expr $vbaCount + 1`
    echo "vba count is $vbaCount"
    done
else
    echo "starting moxi"
    for ((i=0;i<${cmd[2]};i++));do
    pid=$(startMoxi $moxiCount)
    echo "pid is", $pid
    moxiArray[${cmd[2]}]=pid
    moxiCount=`expr $moxiCount + 1`
    done
fi
;;

"stop")
if [[ ${cmd[1]} == "VBA" ]];then
    echo $vbaArray
    pid=${vbaArray[${cmd[2]}]}
    echo $pid
    stopVBA $pid
else
    pid=moxiArray[${cmd[2]}]
    stopMoxi $pid
fi
;;

"serverdown")
echo "call server down for", ${cmd[1]}
curl -s -d "{\"Server\":\"${cmd[1]}\"}" http://localhost:6060/cluster1/serverDown -H "Content-type:application/json"
echo "line is", $line
;;

"serveralive")
curl -s -d "{\"Server\":\"${cmd[1]}\"}" http://localhost:6060/cluster1/serverAlive -H "Content-type:application/json"
echo "call server alive for", ${cmd[1]}
;;

"vbucketmap")
call1="curl -s http://localhost:6060/vbucketMap"
#echo "call vbucketMap"
m=$($call1)
echo "got map as",$m
;;
esac
done
