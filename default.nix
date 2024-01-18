with (import <nixpkgs> {});

stdenv.mkDerivation {
  name = "poetry-env";
  buildInputs = [
    # System requirements.
    readline

    # TODO: hacky non-poetry install stuff we need to get rid of!!
    poetry
    # virtualenv
    # setuptools
    # pip

    # Python requirements (enough to get a virtualenv going).
    python311Full

    # obviously, and see below for hacked linking
    python311Packages.pyqt5
    python311Packages.pyqt5_sip
    # python311Packages.qtpy

    # numerics deps
    python311Packages.levenshtein
    python311Packages.fastparquet
    python311Packages.polars

  ];
  # environment.sessionVariables = {
  #   LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";
  # };
  src = null;
  shellHook = ''
    # Allow the use of wheels.
    SOURCE_DATE_EPOCH=$(date +%s)

    # Augment the dynamic linker path
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${R}/lib/R/lib:${readline}/lib
    export QT_QPA_PLATFORM_PLUGIN_PATH="${qt5.qtbase.bin}/lib/qt-${qt5.qtbase.version}/plugins";

    if [ ! -d ".venv" ]; then
        poetry install --with uis
    fi

    poetry shell
  '';
}
