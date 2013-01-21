#!/bin/bash -xe
if [[ -f /etc/logrotate.d/vbs ]]; then
	echo "Log rotation already configured for vbs logs"
else
	cat >/etc/logrotate.d/vbs <<'EOF'
/var/log/vbs.log {
        notifempty
	daily
	missingok
        size 100M
        rotate 4
        compress
        postrotate 
                /bin/kill -HUP `cat /var/run/syslogd.pid 2> /dev/null` 2> /dev/null || true
        endscript
}

EOF
	echo "Updated log rotation for vbs logs"
fi

exit 0

