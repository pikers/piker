# piker: trading gear for hackers
# Copyright (C) Guillermo Rodriguez (in stewardship for piker0)

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from typing import Union

import trio
import struct


class TectonicDB:

    def __init__(self, host="localhost", port=9001):
        self.host = host
        self.port = port

    async def connect(self):
        self.stream = await trio.open_tcp_stream(
            self.host, self.port
        )

    async def cmd(self, cmd: Union[str, bytes]):
        if isinstance(cmd, str):
            msg = f'{cmd}\n'.encode()

        else:
            msg = cmd + b'\n'

        print(msg)

        await self.stream.send_all(msg)
        
        return await self._recv_text()

    async def _recv_text(self):
        header = await self.stream.receive_some(max_bytes=9)
        current_len = len(header)
        while current_len < 9:
            header += await self.stream.receive_some(max_bytes=9 - current_len)
            current_len = len(header)

        success, bytes_to_read = struct.unpack('>?Q', header)
        if bytes_to_read == 0:
            return success, ""

        body = await self.stream.receive_some(max_bytes=1)
        body_len = len(body)
        while body_len < bytes_to_read:
            len_to_read = bytes_to_read - body_len
            if len_to_read > 32:
                len_to_read = 32
            body += await self.stream.receive_some(max_bytes=len_to_read)
            body_len = len(body)

        return success, body.decode('utf-8')

    def destroy(self):
        self.stream.aclose()

    async def info(self):
        return await self.cmd("INFO")

    async def countall(self):
        return await self.cmd("COUNT ALL")

    async def countall_in_mem(self):
        return await self.cmd("COUNT ALL IN MEM")

    async def ping(self):
        return await self.cmd("PING")

    async def help(self):
        return await self.cmd("HELP")

    async def insert(self, ts, seq, is_trade, is_bid, price, size, dbname):
        return await self.cmd("INSERT {}, {}, {} ,{}, {}, {}; INTO {}"
                        .format( ts, seq,
                            't' if is_trade else 'f',
                            't' if is_bid else 'f', price, size,
                            dbname))

    async def add(self, ts, seq, is_trade, is_bid, price, size):
        return await self.cmd("ADD {}, {}, {} ,{}, {}, {};"
                        .format( ts, seq,
                            't' if is_trade else 'f',
                            't' if is_bid else 'f', price, size))

    async def getall(self):
        success, ret = await self.cmd("GET ALL")
        return success, list(map(lambda x:x.to_dict(), ret))

    async def get(self, n):
        success, ret = await self.cmd("GET {}".format(n))
        if success:
            return success, list(map(lambda x:x.to_dict(), ret))
        else:
            return False, None

    async def clear(self):
        return await self.cmd("CLEAR")

    async def clearall(self):
        return await self.cmd("CLEAR ALL")

    async def flush(self):
        return await self.cmd("FLUSH")

    async def flushall(self):
        return await self.cmd("FLUSH ALL")

    async def create(self, dbname):
        return await self.cmd("CREATE {}".format(dbname))

    async def use(self, dbname):
        return await self.cmd("USE {}".format(dbname))

    async def unsubscribe(self):
        await self.cmd("UNSUBSCRIBE")
        self.subscribed = False

    async def subscribe(self, dbname):
        res = await self.cmd("SUBSCRIBE {}".format(dbname))
        if res[0]:
            self.subscribed = True
        return res

    async def poll(self):
        return await self.cmd("")

    async def range(self, dbname, start, finish):
        self.use(dbname)
        data = await self.cmd("GET ALL FROM {} TO {} AS CSV".format(start, finish).encode())
        data = data[1]
        return data
