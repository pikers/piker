# NOTE: to convert to a poetry2nix env like this here are the
# steps:
# - install poetry in your system nix config
# - convert the repo to use poetry using `poetry init`:
#   https://python-poetry.org/docs/basic-usage/#initialising-a-pre-existing-project
# - then manually ensuring all deps are converted over:
# - add this file to the repo and commit it
# - 
{
  description = "piker: trading gear for hackers (pkged with poetry2nix)";

  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  # see https://github.com/nix-community/poetry2nix/tree/master#api
  inputs.poetry2nix = {
    # url = "github:nix-community/poetry2nix";
    # url = "github:K900/poetry2nix/qt5-explicit-deps";
    url = "/home/lord_fomo/repos/poetry2nix";

    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    poetry2nix,
  }:
    # TODO: build cross-OS and use the `${system}` var thingy..
    flake-utils.lib.eachDefaultSystem (system:
      let
        # use PWD as sources
        projectDir = ./.;
        pyproject = ./pyproject.toml;
        poetrylock = ./poetry.lock;

        # TODO: port to 3.11 and support both versions?
        python = "python3.10";

        # for more functions and examples.
        # inherit
        # (poetry2nix.legacyPackages.${system})
        # mkPoetryApplication;
        # pkgs = nixpkgs.legacyPackages.${system};

        pkgs = nixpkgs.legacyPackages.x86_64-linux;
        lib = pkgs.lib;
        p2npkgs = poetry2nix.legacyPackages.x86_64-linux;

        # define all pkg overrides per dep, see edgecases.md:
        # https://github.com/nix-community/poetry2nix/blob/master/docs/edgecases.md
        # TODO: add these into the json file:
        # https://github.com/nix-community/poetry2nix/blob/master/overrides/build-systems.json
        pypkgs-build-requirements = {
          asyncvnc = [ "setuptools" ];
          eventkit = [ "setuptools" ];
          ib-insync = [ "setuptools" "flake8" ];
          msgspec = [ "setuptools"];
          pdbp = [ "setuptools" ];
          pyqt6-sip = [ "setuptools" ];
          tabcompleter = [ "setuptools" ];
          tractor = [ "setuptools" ];
          tricycle = [ "setuptools" ];
          trio-typing = [ "setuptools" ];
          trio-util = [ "setuptools" ];
          xonsh = [ "setuptools" ];
        };

        # auto-generate override entries
        p2n-overrides = p2npkgs.defaultPoetryOverrides.extend (self: super:
          builtins.mapAttrs (package: build-requirements:
            (builtins.getAttr package super).overridePythonAttrs (old: {
              buildInputs = (
                old.buildInputs or [ ]
              ) ++ (
                builtins.map (
                  pkg: if builtins.isString pkg then builtins.getAttr pkg super else pkg
                  ) build-requirements
              );
            })
          ) pypkgs-build-requirements
        );

        # override some ahead-of-time compiled extensions
        # to be built with their wheels.
        ahot_overrides = p2n-overrides.extend(
          final: prev: {

            # llvmlite = prev.llvmlite.override {
            #   preferWheel = false;
            # };

            # TODO: get this workin with p2n and nixpkgs..
            # pyqt6 = prev.pyqt6.override {
            #   preferWheel = true;
            # };

            # NOTE: this DOESN'T work atm but after a fix
            # to poetry2nix, it will and actually this line
            # won't be needed - thanks @k900:
            # https://github.com/nix-community/poetry2nix/pull/1257
            pyqt5 = prev.pyqt5.override {
              withWebkit = false;
              preferWheel = true;
            };

            # see PR from @k900:
            # https://github.com/nix-community/poetry2nix/pull/1257
            # pyqt5-qt5 = prev.pyqt5-qt5.override {
            #   withWebkit = false;
            #   preferWheel = true;
            # };

            # TODO: patch in an override for polars to build
            # from src! See the details likely needed from
            # the cryptography entry:
            # https://github.com/nix-community/poetry2nix/blob/master/overrides/default.nix#L426-L435
            polars = prev.polars.override {
              preferWheel = true;
            };
          }
      );

      # WHY!? -> output-attrs that `nix develop` scans for:
      # https://nixos.org/manual/nix/stable/command-ref/new-cli/nix3-develop.html#flake-output-attributes
      in {
          packages = {
            # piker = poetry2nix.legacyPackages.x86_64-linux.mkPoetryEditablePackage {
            #   editablePackageSources = { piker = ./piker; };

            piker = p2npkgs.mkPoetryApplication {
              projectDir = projectDir;

              # SEE ABOVE for auto-genned input set, override
              # buncha deps with extras.. like `setuptools` mostly.
              # TODO: maybe propose a patch to p2n to show that you
              # can even do this in the edgecases docs?
              overrides = ahot_overrides;

              # XXX: won't work on llvmlite..
              # preferWheels = true;
            };
          };

          devShells.default = pkgs.mkShell {
            # packages = [ poetry2nix.packages.${system}.poetry ];
            packages = [ poetry2nix.packages.x86_64-linux.poetry ];
            inputsFrom = [ self.packages.x86_64-linux.piker ];

            # TODO: boot xonsh inside the poetry virtualenv when
            # defined via a custom entry point?
            # NOTE XXX: apparently DON'T do these..?
            # shellHook = "poetry run xonsh";
            # shellHook = "poetry shell";
          };


          # TODO: grok the difference here..
          # - avoid re-cloning git repos on every develop entry..
          # - ideally allow hacking on the src code of some deps
          #   (tractor, pyqtgraph, tomlkit, etc.) WITHOUT having to
          #   re-install them every time a change is made.

          # devShells.default = (p2npkgs.mkPoetryEnv {
          # # let {
          # #   devEnv = p2npkgs.mkPoetryEnv {
          #     projectDir = projectDir;
          #     overrides = ahot_overrides;
          #     inputsFrom = [ self.packages.x86_64-linux.piker ];
          #   }).env.overrideAttrs (old: {
          #       buildInputs = [ packages.piker ];
          #     }
          #   );

        }
    );  # end of .outputs scope
}
