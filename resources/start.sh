#!/usr/bin/env bash

set -euo pipefail

unset PIDS

PIDS=""
UWSGI_INI_LOCATION=/etc/uwsgi/uwsgi.ini

trap cleanup 1 2 3 9 15

function cleanup() {
    echo ""
    echo "cleaning up ..."
    echo ""
    kill -s 9 ${PIDS}
}

function run_uwsgi() {
    echo "starting uwsgi ..."
    uwsgi=$(which uwsgi)
    exec ${uwsgi} --ini ${UWSGI_INI_LOCATION} 1>&2 &
    pid=$!
    echo "with process id ${pid}"
    PIDS="${PIDS} $pid"
    unset pid
}

function run_nginx() {
    nginx=$(which nginx)
    exec ${nginx} 1>&2 &
    PIDS="${PIDS} $!"
    pid=$!
    echo "with process id ${pid}"
    unset pid
}

run_uwsgi
run_nginx
wait