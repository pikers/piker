#!/bin/sh
# start vnc server and listen for connections
# on port specced in `$VNC_SERVER_PORT`

x11vnc \
    -listen 127.0.0.1 \
    -allow 127.0.0.1 \
    -rfbport "${VNC_SERVER_PORT}" \
    -display :1 \
    -forever \
    -shared \
    -bg \
    -nowf \
    -noxdamage \
    -noxfixes \
    -no6 \
    -noipv6 \


    # -nowcr \
    # TODO: can't use this because of ``asyncvnc`` issue:
    # https://github.com/barneygale/asyncvnc/issues/1
    # -passwd 'ibcansmbz'

    # XXX: optional graphics caching flags that seem to rekt the overlay
    # of the 2 gw windows? When running a single gateway
    # this seems to maybe optimize some memory usage?
    # -ncache_cr \
    # -ncache \

    # NOTE: this will prevent logs from going to the console.
    # -logappend /var/log/x11vnc.log \

    # where to start allocating ports
    # -autoport "${VNC_SERVER_PORT}" \
