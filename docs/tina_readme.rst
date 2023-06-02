### NOTE this is likely out of date given it was written some
(years) time ago by a user that has since not really partaken in
contributing since.

install for tinas
*****************
for windows peeps you can start by installing all the prerequisite software:

- install git with all default settings - https://git-scm.com/download/win
- install anaconda all default settings - https://www.anaconda.com/products/individual
- install microsoft build tools (check the box for Desktop development for C++, you might be able to uncheck some optional downloads)  - https://visualstudio.microsoft.com/visual-cpp-build-tools/
- install visual studio code default settings - https://code.visualstudio.com/download


then, `crack a conda shell`_ and run the following commands::

    mkdir code # create code directory
    cd code # change directory to code
    git clone https://github.com/pikers/piker.git # downloads piker installation package from github
    cd piker # change directory to piker
    
    conda create -n pikonda # creates conda environment named pikonda
    conda activate pikonda # activates pikonda
    
    conda install -c conda-forge python-levenshtein # in case it is not already installed
    conda install pip # may already be installed
    pip # will show if pip is installed
    
    pip install -e . -r requirements.txt # install piker in editable mode

test Piker to see if it is working::

    piker -b binance chart btcusdt.binance # formatting for loading a chart
    piker -b kraken -b binance chart xbtusdt.kraken
    piker -b kraken -b binance -b ib chart qqq.nasdaq.ib
    piker -b ib chart tsla.nasdaq.ib

potential error::
    
    FileNotFoundError: [Errno 2] No such file or directory: 'C:\\Users\\user\\AppData\\Roaming\\piker\\brokers.toml'
    
solution:

- navigate to file directory above (may be different on your machine, location should be listed in the error code)
- copy and paste file from 'C:\\Users\\user\\code\\data/brokers.toml' or create a blank file using notepad at the location above

Visual Studio Code setup:

- now that piker is installed we can set up vscode as the default terminal for running piker and editing the code
- open Visual Studio Code
- file --> Add Folder to Workspace --> C:\Users\user\code\piker (adds piker directory where all piker files are located)
- file --> Save Workspace As --> save it wherever you want and call it whatever you want, this is going to be your default workspace for running and editing piker code
- ctrl + shift + p --> start typing Python: Select Interpetter --> when the option comes up select it --> Select at the workspace level --> select the one that shows ('pikonda')
- change the default terminal to cmd.exe instead of powershell (default)
- now when you create a new terminal VScode should automatically activate you conda env so that piker can be run as the first command after a new terminal is created

also, try out fancyzones as part of powertoyz for a decent tiling windows manager to manage all the cool new software you are going to be running.

.. _conda installed: https://
.. _C++ build toolz: https://
.. _crack a conda shell: https://
.. _vscode: https://

.. link to the tina guide
.. _setup a coolio tiled wm console: https://

provider support
****************
for live data feeds the in-progress set of supported brokers is:

- IB_ via ``ib_insync``, also see our `container docs`_
- binance_ and kraken_ for crypto over their public websocket API
- questrade_ (ish) which comes with effectively free L1

coming soon...

- webull_ via the reverse engineered public API
- yahoo via yliveticker_

if you want your broker supported and they have an API let us know.

.. _IB: https://interactivebrokers.github.io/tws-api/index.html
.. _container docs: https://github.com/pikers/piker/tree/master/dockering/ib
.. _questrade: https://www.questrade.com/api/documentation
.. _kraken: https://www.kraken.com/features/api#public-market-data
.. _binance: https://github.com/pikers/piker/pull/182
.. _webull: https://github.com/tedchou12/webull
.. _yliveticker: https://github.com/yahoofinancelive/yliveticker
.. _coinbase: https://docs.pro.coinbase.com/#websocket-feed


