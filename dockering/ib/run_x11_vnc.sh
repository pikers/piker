#!/bin/sh

x11vnc \
    -ncache_cr \
    -display :1 \
    -forever \
    -shared \
    -logappend /var/log/x11vnc.log \
    -bg \
    -noipv6 \
    # can't use this because of ``asyncvnc`` issue:
    # https://github.com/barneygale/asyncvnc/issues/1
    # -passwd "$VNC_SERVER_PASSWORD"
