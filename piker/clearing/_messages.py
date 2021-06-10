# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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

"""
Clearing system messagingn types and protocols.

"""
from typing import Optional, Union

# TODO: try out just encoding/send direction for now?
# import msgspec
from pydantic import BaseModel

# Client -> emsd


class Cancel(BaseModel):
    '''Cancel msg for removing a dark (ems triggered) or
    broker-submitted (live) trigger/order.

    '''
    action: str = 'cancel'
    oid: str  # uuid4
    symbol: str


class Order(BaseModel):

    action: str  # {'buy', 'sell', 'alert'}
    # internal ``emdsd`` unique "order id"
    oid: str  # uuid4
    symbol: str

    price: float
    size: float
    brokers: list[str]

    # Assigned once initial ack is received
    # ack_time_ns: Optional[int] = None

    # determines whether the create execution
    # will be submitted to the ems or directly to
    # the backend broker
    exec_mode: str  # {'dark', 'live', 'paper'}


# Client <- emsd
# update msgs from ems which relay state change info
# from the active clearing engine.


class Status(BaseModel):

    name: str = 'status'
    oid: str  # uuid4
    time_ns: int

    # {
    #   'dark_submitted',
    #   'dark_cancelled',
    #   'dark_triggered',

    #   'broker_submitted',
    #   'broker_cancelled',
    #   'broker_executed',
    #   'broker_filled',

    #   'alert_submitted',
    #   'alert_triggered',

    #   'position',

    # }
    resp: str  # "response", see above

    # symbol: str

    # trigger info
    trigger_price: Optional[float] = None
    # price: float

    # broker: Optional[str] = None

    # this maps normally to the ``BrokerdOrder.reqid`` below, an id
    # normally allocated internally by the backend broker routing system
    broker_reqid: Optional[Union[int, str]] = None

    # for relaying backend msg data "through" the ems layer
    brokerd_msg: dict = {}


# emsd -> brokerd
# requests *sent* from ems to respective backend broker daemon

class BrokerdCancel(BaseModel):

    action: str = 'cancel'
    oid: str  # piker emsd order id
    time_ns: int

    # "broker request id": broker specific/internal order id if this is
    # None, creates a new order otherwise if the id is valid the backend
    # api must modify the existing matching order. If the broker allows
    # for setting a unique order id then this value will be relayed back
    # on the emsd order request stream as the ``BrokerdOrderAck.reqid``
    # field
    reqid: Optional[Union[int, str]] = None


class BrokerdOrder(BaseModel):

    action: str  # {buy, sell}
    oid: str
    time_ns: int

    # "broker request id": broker specific/internal order id if this is
    # None, creates a new order otherwise if the id is valid the backend
    # api must modify the existing matching order. If the broker allows
    # for setting a unique order id then this value will be relayed back
    # on the emsd order request stream as the ``BrokerdOrderAck.reqid``
    # field
    reqid: Optional[Union[int, str]] = None

    symbol: str  # symbol.<providername> ?
    price: float
    size: float


# emsd <- brokerd
# requests *received* to ems from broker backend


class BrokerdOrderAck(BaseModel):
    '''Immediate reponse to a brokerd order request providing
    the broker specifci unique order id.

    '''
    name: str = 'ack'

    # defined and provided by backend
    reqid: Union[int, str]

    # emsd id originally sent in matching request msg
    oid: str


class BrokerdStatus(BaseModel):

    name: str = 'status'
    reqid: Union[int, str]
    time_ns: int

    # {
    #   'submitted',
    #   'cancelled',
    #   'executed',
    # }
    status: str

    filled: float = 0.0
    reason: str = ''
    remaining: float = 0.0

    # XXX: better design/name here?
    # flag that can be set to indicate a message for an order
    # event that wasn't originated by piker's emsd (eg. some external
    # trading system which does it's own order control but that you
    # might want to "track" using piker UIs/systems).
    external: bool = False

    # XXX: not required schema as of yet
    broker_details: dict = {
        'name': '',
    }


class BrokerdFill(BaseModel):
    '''A single message indicating a "fill-details" event from the broker
    if avaiable.

    '''
    name: str = 'fill'
    reqid: Union[int, str]
    time_ns: int

    # order exeuction related
    action: str
    size: float
    price: float

    broker_details: dict = {}  # meta-data (eg. commisions etc.)

    # brokerd timestamp required for order mode arrow placement on x-axis

    # TODO: maybe int if we force ns?
    # we need to normalize this somehow since backends will use their
    # own format and likely across many disparate epoch clocks...
    broker_time: float


class BrokerdError(BaseModel):
    '''Optional error type that can be relayed to emsd for error handling.

    This is still a TODO thing since we're not sure how to employ it yet.
    '''
    name: str = 'error'
    reqid: Union[int, str]

    symbol: str
    reason: str
    broker_details: dict = {}


class BrokerdPosition(BaseModel):
    '''Position update event from brokerd.

    '''
    name: str = 'position'

    broker: str
    account: str
    symbol: str
    currency: str
    size: float
    avg_price: float
