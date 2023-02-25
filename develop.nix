with (import <nixpkgs> {});
with python311Packages;
stdenv.mkDerivation {
  name = "pip-env";
  buildInputs = [
    # System requirements.
    readline

    # Python requirements (enough to get a virtualenv going).
    python311
    virtualenv
    setuptools
    pyqt5
    pip
  ];
  src = null;
  shellHook = ''
    # Allow the use of wheels.
    SOURCE_DATE_EPOCH=$(date +%s)

    # Augment the dynamic linker path
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${R}/lib/R/lib:${readline}/lib

    export QT_QPA_PLATFORM_PLUGIN_PATH="${qt5.qtbase.bin}/lib/qt-${qt5.qtbase.version}/plugins";

    if [ ! -d "venv" ]; then
        virtualenv venv
    fi

    source venv/bin/activate
  '';
}
