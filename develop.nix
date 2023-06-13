with (import <nixpkgs> {});
with python310Packages;
stdenv.mkDerivation {
  name = "pip-env";
  buildInputs = [
    # System requirements.
    readline

    # TODO: hacky non-poetry install stuff we need to get rid of!!
    virtualenv
    setuptools
    pip

    # obviously, and see below for hacked linking
    pyqt5

    # Python requirements (enough to get a virtualenv going).
    python310Full

    # numerics deps
    python310Packages.python-Levenshtein
    python310Packages.fastparquet
    python310Packages.polars

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
