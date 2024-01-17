with (import <nixpkgs> {});
with python311Packages;
let
  gccStorePath = lib.getLib gcc-unwrapped;
  glibStorePath = lib.getLib glib;
  libglStorePath = lib.getLib libglvnd;

  fconfStorePath = lib.getLib fontconfig;
  freetypeStorePath = lib.getLib freetype;
  x11StorePath = lib.getLib xorg.libX11;
  xcbwmStorePath = lib.getLib xorg.xcbutilwm;
  xcbimgStorePath = lib.getLib xorg.xcbutilimage;
  libxcbStorePath = lib.getLib xorg.libxcb;
  xcbutilStorePath = lib.getLib xorg.xcbutil;
  xcbutilkeysStorePath = lib.getLib xorg.xcbutilkeysyms;
  xcbutilrenderStorePath = lib.getLib xorg.xcbutilrenderutil;

  libxkbcomStorePath = lib.getLib libxkbcommon;

  dbusStorePath = lib.getLib dbus;

  pyqt5StorePath = lib.getLib pyqt5;
  pyqt5SipStorePath = lib.getLib pyqt5_sip;
in
stdenv.mkDerivation {
  name = "piker-poetry-shell-with-qt-fix";
  buildInputs = [
    # System requirements.
    gcc-unwrapped
    glib

    readline
    fontconfig
    freetype
    xorg.libX11
    xorg.xcbutilwm
    xorg.xcbutilimage
    xorg.libxcb
    xorg.xcbutil
    xorg.xcbutilkeysyms
    xorg.xcbutilrenderutil

    libxkbcommon

    libsForQt5.qt5.qtbase

    # Python requirements.
    python311Full
    poetry-core
    virtualenv
    pyqt5
  ];
  src = null;
  shellHook = ''
    set -e

    # Allow the use of wheels.
    SOURCE_DATE_EPOCH=$(date +%s)

    GCC_STORE_PATH="${gccStorePath}/lib"
    GLIB_STORE_PATH="${glibStorePath}/lib"
    OPENGL_PATH="${libglStorePath}/lib"

    FONTCONFIG_PATH="${fconfStorePath}/lib"
    FREETYPE_PATH="${freetypeStorePath}/lib"
    LIBX11_PATH="${x11StorePath}/lib"
    XCBWM_PATH="${xcbwmStorePath}/lib"
    XCBIMG_PATH="${xcbimgStorePath}/lib"
    LIBXCB_PATH="${libxcbStorePath}/lib"
    XCBUTL_PATH="${xcbutilStorePath}/lib"
    XCBUTLKS_PATH="${xcbutilkeysStorePath}/lib"
    XCBUTLRNDR_PATH="${xcbutilrenderStorePath}/lib"

    LIBXKBCOM_PATH="${libxkbcomStorePath}/lib"

    DBUS_PATH="${dbusStorePath}/lib"

    QTBASE_PATH="${qt5.qtbase.bin}/lib/qt-${qt5.qtbase.version}"

    READLINE_PATH="${readline}/lib"

    echo "readline path:      $READLINE_PATH"
    echo ""
    echo "gcc store path:     $GCC_STORE_PATH"
    echo "glib store path:    $GLIB_STORE_PATH"
    echo "opengl path:        $OPENGL_PATH"
    echo "fontconfig path:    $FONTCONFIG_PATH"
    echo "freetype path:      $FREETYPE_PATH"
    echo "libX11 path:        $LIBX11_PATH"
    echo "xcbutilwm path:     $XCBWM_PATH"
    echo "xcbutilimage path:  $XCBIMG_PATH"
    echo "libxcb path:        $LIBXCB_PATH"
    echo "xcbutil path:       $XCBUTL_PATH"
    echo "xcbutilkeys path:   $XCBUTLKS_PATH"
    echo "xcbutilrender path: $XCBUTLRNDR_PATH"
    echo "libxkcommon path:   $LIBXKBCOM_PATH"
    echo ""
    echo "qtbase path:        $QTBASE_PATH"
    echo ""
    echo "dbus path:          $DBUS_PATH"
    echo ""

    EXTRA_LD_PATHS=""
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$READLINE_PATH"

    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$GCC_STORE_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$GLIB_STORE_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$OPENGL_PATH"

    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$FONTCONFIG_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$FREETYPE_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$LIBX11_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$XCBWM_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$XCBIMG_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$LIBXCB_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$XCBUTL_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$XCBUTLKS_PATH"
    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$XCBUTLRNDR_PATH"

    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$LIBXKBCOM_PATH"

    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$DBUS_PATH"

    EXTRA_LD_PATHS="$EXTRA_LD_PATHS:$QTBASE_PATH"

    # Augment the dynamic linker path
    export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$EXTRA_LD_PATHS"

    # Set the Qt plugin path
    # export QT_DEBUG_PLUGINS=1
    export QT_PLUGIN_PATH="$QTBASE_PATH/plugins"
    export QT_QPA_PLATFORM_PLUGIN_PATH="$QT_PLUGIN_PATH/platforms"
    echo "qt plguin path:     $QT_PLUGIN_PATH"
    echo ""

    # Maybe create venv & install deps
    poetry install --with=nixos

    # Use pyqt5 from System, patch activate script
    ACTIVATE_SCRIPT_PATH="$(poetry env info --path)/bin/activate"
    export PYQT5_PATH="${pyqt5StorePath}/lib/python3.11/site-packages"
    export PYQT5_SIP_PATH="${pyqt5SipStorePath}/lib/python3.11/site-packages"
    echo "pyqt5 at:           $PYQT5_PATH"
    echo "pyqt5-sip at:       $PYQT5_SIP_PATH"
    echo ""

    PATCH="export PYTHONPATH=\"\$PYQT5_PATH:\$PYQT5_SIP_PATH\""
    if grep -q "$PATCH" "$ACTIVATE_SCRIPT_PATH"; then
        echo "venv is already patched."
    else
        echo "patching $ACTIVATE_SCRIPT_PATH to use pyqt5 from nixos..."
        sed -i "\$i$PATCH" $ACTIVATE_SCRIPT_PATH
    fi

    poetry shell
  '';
}
