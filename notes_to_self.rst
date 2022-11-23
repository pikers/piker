Notes to self
=============
chicken scratch we shan't forget, consider this staging
for actual feature issues on wtv git wrapper-provider we're
using (no we shan't stick with GH long term likely).


cool chart features
-------------------
- allow right-click to spawn shell with current in view
  data passed to the new process via ``msgpack-numpy``.
- expand OHLC datum to lower time frame.
- auto-highlight current time range on tick feed


IB Ideas
-------------------------
- vlm diffing from ticks and compare when bar arrives from historical
  - should help isolate dark vlm / trades

IB TWS protips
-------------------------
- for realtime derivatives (calls/puts) charting, select midpoint and candlestick under chart 
  parameters for a big performance boost when trying to load a multitude of different assets and charts.
  

chart ux ideas
--------------
- hotkey to zoom to order intersection (horizontal line) with previous
  price levels (+ some margin obvs).
- L1 "lines" (queue size repr) should normalize to some fixed x width
  such that when levels with more vlm appear other smaller levels are
  scaled down giving an immediate indication of the liquidity diff.
