#!/bin/sh

# start VNC server
x11vnc \
    -listen 127.0.0.1 \
    -allow 127.0.0.1 \
    -autoport 3003 \
    -no6 \
    -noipv6 \
    -display :1 \
    -bg \
    -forever \
    -shared \
    -logappend /var/log/x11vnc.log \
    -ncache_cr \
    -ncache \

    # can't use this because of ``asyncvnc`` issue:
    # https://github.com/barneygale/asyncvnc/issues/1
    # -passwd 'ibcansmbz'
