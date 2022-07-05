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
