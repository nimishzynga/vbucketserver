#! /bin/bash
#
# Source function library.
[[ -f /etc/rc.d/init.d/functions ]] && . /etc/rc.d/init.d/functions

VBS_ROOT=/opt/vbucketserver

PIDFILE="/var/run/${0##*/}".pid

# Check that networking is up.
if [ "$NETWORKING" = "no" ]
then
	exit 0
fi

RETVAL=0
prog="vbucketserver"

start () {
    local pid
    # check if it is running
    if [[ -f "$PIDFILE" ]];then
        read pid < "$PIDFILE"
    fi
    if [[ ! -f "$PIDFILE" || -z "$pid" ]];then
        pid=$(pidof ${VBS_ROOT}/vbucketserver)
    fi
    if [[ -n "$pid" && -d "/proc/$pid" ]];then
      echo "Already running..."
      exit 0
    fi
    ${VBS_ROOT}/vbucketserver >> /var/log/vbs.log 2>&1 &

    pidbg=$!
    rc=$(ps -aef | grep $prog | wc -l)

    if [ $rc -ge 1 ] ; then
      cmd='/bin/true'
      echo "$pidbg" > "$PIDFILE"
    else
      cmd='/bin/false'
      rm -f $PIDFILE
    fi
	action $"Starting $prog: " $cmd
}
stop () {
        local pid
	killproc -p "$PIDFILE" vbs.sh
	echo "Stopping $prog: "
}

status (){
    local pid
    if [[ -f "$PIDFILE" ]];then
        read pid < "$PIDFILE"
        if [[ -n "$pid" && -d "/proc/$pid" ]];then
            echo "VbucketServer is running.."
        else
            echo "VbucketServer is stopped"
        fi
    else
        echo "VbucketServer is stopped"
    fi
}
restart () {
        stop
        start
}

# See how we were called.
case "$1" in
  start)
	start
	;;
  stop)
	stop
	;;
  status)
	status
	;;
  restart|reload)
	restart
	;;
  *)
	echo $"Usage: $0 {start|stop|restart}"
	exit 1
esac

exit $?
