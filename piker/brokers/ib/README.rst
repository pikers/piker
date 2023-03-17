``ib`` backend
--------------
more or less the "everything broker" for traditional and international
markets. they are the "go to" provider for automatic retail trading
and we interface to their APIs using the `ib_insync` project.

status
******
current support is *production grade* and both real-time data and order
management should be correct and fast. this backend is used by core devs
for live trading.

currently there is not yet full support for:
- options charting and trading
- paxos based crypto rt feeds and trading


config
******
In order to get order mode support your ``brokers.toml``
needs to have something like the following:

.. code:: toml

   [ib]
   hosts = [
    "127.0.0.1",
   ]
   # TODO: when we eventually spawn gateways in our
   # container, we can just dynamically allocate these
   # using IBC.
   ports = [
       4002,
       4003,
       4006,
       4001,
       7497,
   ]

   # XXX: for a paper account the flex web query service
   # is not supported so you have to manually download
   # and XML report and put it in a location that can be
   # accessed by the ``brokerd.ib`` backend code for parsing.
   flex_token = '1111111111111111'
   flex_trades_query_id = '6969696'  # live accounts only?

   # 3rd party web-api token
   # (XXX: not sure if this works yet)
   trade_log_token = '111111111111111'

   # when clients are being scanned this determines
   # which clients are preferred to be used for data feeds
   # based on account names which are detected as active
   # on each client.
   prefer_data_account = [
       # this has to be first in order to make data work with dual paper + live
       'main',
       'algopaper',
   ]

   [ib.accounts]
   main = 'U69696969'
   algopaper = 'DU9696969'


If everything works correctly you should see any current positions
loaded in the pps pane on chart load and you should also be able to
check your trade records in the file::

    <pikerk_conf_dir>/ledgers/trades_ib_algopaper.toml


An example ledger file will have entries written verbatim from the
trade events schema:

.. code:: toml

    ["0000e1a7.630f5e5a.01.01"]
    secType = "FUT"
    conId = 515416577
    symbol = "MNQ"
    lastTradeDateOrContractMonth = "20221216"
    strike = 0.0
    right = ""
    multiplier = "2"
    exchange = "GLOBEX"
    primaryExchange = ""
    currency = "USD"
    localSymbol = "MNQZ2"
    tradingClass = "MNQ"
    includeExpired = false
    secIdType = ""
    secId = ""
    comboLegsDescrip = ""
    comboLegs = []
    execId = "0000e1a7.630f5e5a.01.01"
    time = 1661972086.0
    acctNumber = "DU69696969"
    side = "BOT"
    shares = 1.0
    price = 12372.75
    permId = 441472655
    clientId = 6116
    orderId = 985
    liquidation = 0
    cumQty = 1.0
    avgPrice = 12372.75
    orderRef = ""
    evRule = ""
    evMultiplier = 0.0
    modelCode = ""
    lastLiquidity = 1
    broker_time = 1661972086.0
    name = "ib"
    commission = 0.57
    realizedPNL = 243.41
    yield_ = 0.0
    yieldRedemptionDate = 0
    listingExchange = "GLOBEX"
    date = "2022-08-31T18:54:46+00:00"


your ``pps.toml`` file will have position entries like,

.. code:: toml

    [ib.algopaper."mnq.globex.20221216"]
    size = -1.0
    ppu = 12423.630576923071
    bs_mktid = 515416577
    expiry = "2022-12-16T00:00:00+00:00"
    clears = [
     { dt = "2022-08-31T18:54:46+00:00", ppu = 12423.630576923071, accum_size = -19.0, price = 12372.75, size = 1.0, cost = 0.57, tid = "0000e1a7.630f5e5a.01.01" },
    ]
