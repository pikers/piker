running ``ib`` gateway in ``docker``
------------------------------------
We have a config based on the (now defunct)
image from "waytrade":

https://github.com/waytrade/ib-gateway-docker

To startup this image with our custom settings
simply run the command::

    docker compose up

And you should have the following socket-available services:

- ``x11vnc1@127.0.0.1:3003``
- ``ib-gw@127.0.0.1:4002``

You can attach to the container via a VNC client
without password auth.

SECURITY STUFF!?!?!
-------------------
Though "``ib``" claims they host filter connections outside
localhost (aka ``127.0.0.1``) it's probably better if you filter
the socket at the OS level using a stateless firewall rule::

    ip rule add not unicast iif lo to 0.0.0.0/0 dport 4002

We will soon have this baked into our own custom image but for
now you'll have to do it urself dawgy.
