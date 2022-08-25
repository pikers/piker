#
# to run this image: https://github.com/mviereck/x11docker
#
#     x11docker \
#         --network=host \
#         --gpu -- \
#         piker:master \
#         piker -b binance chart btcbusd.binance
#

FROM python:3.10.6-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y \
        git \
        libgl1 \
        libxcb-* \
        libdbus-1-3 \
        libglib2.0-0 \
        build-essential \
        libxkbcommon-x11-0

# manually doing activation makes all docker cmds work with env
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /home

RUN git clone https://github.com/pikers/piker.git

# create non root user
RUN useradd -s /bin/bash piker && \
    chown -R piker:piker /home/piker && \
    chown -R piker:piker $VIRTUAL_ENV

USER piker

WORKDIR /home/piker

# finally install piker
RUN pip install -U pip && \
    pip install -r requirements.txt .
