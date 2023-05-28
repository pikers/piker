``kraken`` backend
------------------
though they don't have the most liquidity of all the cexes they sure are
accommodating to those of us who appreciate a little ``xmr``.

status
******
current support is *production grade* and both real-time data and order
management should be correct and fast. this backend is used by core devs
for live trading.


config
******
In order to get order mode support your ``brokers.toml``
needs to have something like the following:

.. code:: toml

   [kraken]
   accounts.spot = 'spot'
   key_descr = "spot"
   api_key = "69696969696969696696969696969696969696969696969696969696"
   secret = "BOOBSBOOBSBOOBSBOOBSBOOBSSMBZ69696969696969669969696969696"


If everything works correctly you should see any current positions
loaded in the pps pane on chart load and you should also be able to
check your trade records in the file::

    <pikerk_conf_dir>/ledgers/trades_kraken_spot.toml


An example ledger file will have entries written verbatim from the
trade events schema:

.. code:: toml

    [TFJBKK-SMBZS-VJ4UWS]
    ordertxid = "SMBZSA-7CNQU-3HWLNJ"
    postxid = "SMBZSE-M7IF5-CFI7LT"
    pair = "XXMRZEUR"
    time = 1655691993.4133966
    type = "buy"
    ordertype = "limit"
    price = "103.97000000"
    cost = "499.99999977"
    fee = "0.80000000"
    vol = "4.80907954"
    margin = "0.00000000"
    misc = ""


your ``pps.toml`` file will have position entries like,

.. code:: toml

   [kraken.spot."xmreur.kraken"]
   size = 4.80907954
   ppu = 103.97000000
   bs_mktid = "XXMRZEUR"
   clears = [
    { tid = "TFJBKK-SMBZS-VJ4UWS", cost = 0.8, price = 103.97, size = 4.80907954, dt = "2022-05-20T02:26:33.413397+00:00" },
   ]
