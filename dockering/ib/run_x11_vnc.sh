#!/bin/sh

# start VNC server
x11vnc \
    -ncache_cr \
    -listen localhost \
    -display :1 \
    -forever \
    -shared \
    -logappend /var/log/x11vnc.log \
    -bg \
    -noipv6 \
    -autoport 3003 \
    # can't use this because of ``asyncvnc`` issue:
    # https://github.com/barneygale/asyncvnc/issues/1
    # -passwd "$VNC_SERVER_PASSWORD"
