with (import <nixpkgs> {});
with python311Packages;
let
  rapidfuzzStorePath = lib.getLib rapidfuzz;
  qdarkstyleStorePath = lib.getLib qdarkstyle;
  pyqt5StorePath = lib.getLib pyqt5;
  pyqt5SipStorePath = lib.getLib pyqt5_sip;
in
stdenv.mkDerivation {
  name = "piker-poetry-shell-with-qt-fix";
  buildInputs = [
    # System requirements.
    libsForQt5.qt5.qtbase

    # Python requirements.
    python311Full
    poetry-core
    rapidfuzz
    qdarkstyle
    pyqt5
  ];
  src = null;
  shellHook = ''
    set -e

    QTBASE_PATH="${qt5.qtbase.bin}/lib/qt-${qt5.qtbase.version}"

    # Set the Qt plugin path
    # export QT_DEBUG_PLUGINS=1
    export QT_PLUGIN_PATH="$QTBASE_PATH/plugins"
    export QT_QPA_PLATFORM_PLUGIN_PATH="$QT_PLUGIN_PATH/platforms"

    # Maybe create venv & install deps
    poetry install --with=nix-shell

    # Use pyqt5 from System, patch activate script
    ACTIVATE_SCRIPT_PATH="$(poetry env info --path)/bin/activate"
    export RPDFUZZ_PATH="${rapidfuzzStorePath}/lib/python3.11/site-packages"
    export QDRKSTYLE_PATH="${qdarkstyleStorePath}/lib/python3.11/site-packages"
    export PYQT5_PATH="${pyqt5StorePath}/lib/python3.11/site-packages"
    export PYQT5_SIP_PATH="${pyqt5SipStorePath}/lib/python3.11/site-packages"
    echo "rapidfuzz at:   $RPDFUZZ_PATH"
    echo "qdarkstyle at:  $QDRKSTYLE_PATH"
    echo "pyqt5 at:       $PYQT5_PATH"
    echo "pyqt5-sip at:   $PYQT5_SIP_PATH"
    echo ""

    PATCH="export PYTHONPATH=\""

    PATCH="$PATCH\$RPDFUZZ_PATH"
    PATCH="$PATCH:\$QDRKSTYLE_PATH"
    PATCH="$PATCH:\$PYQT5_PATH"
    PATCH="$PATCH:\$PYQT5_SIP_PATH"

    PATCH="$PATCH\""

    if grep -q "$PATCH" "$ACTIVATE_SCRIPT_PATH"; then
        echo "venv is already patched."
    else
        echo "patching $ACTIVATE_SCRIPT_PATH to use pyqt5 from nixos..."
        sed -i "\$i$PATCH" $ACTIVATE_SCRIPT_PATH
    fi

    echo "qt plguin path: $QT_PLUGIN_PATH"
    echo ""

    poetry shell
  '';
}
