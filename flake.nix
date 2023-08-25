# NOTE: to convert to a poetry2nix env like this here are the
# steps:
# - install poetry in your system nix config
# - convert the repo to use poetry using `poetry init`:
#   https://python-poetry.org/docs/basic-usage/#initialising-a-pre-existing-project
# - then manually ensuring all deps are converted over:
{
  description = "piker: trading gear for hackers (pkged with poetry2nix)";

  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.poetry2nix = {
    url = "github:nix-community/poetry2nix";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    poetry2nix,
  }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        # use PWD as sources
        projectDir = ./.;
        pyproject = ./pyproject.toml;
        poetrylock = ./poetry.lock;

        # TODO: port to 3.11 and support both versions?
        python = "python3.10";

        # see https://github.com/nix-community/poetry2nix/tree/master#api
        # for more functions and examples.
        # inherit
        # (poetry2nix.legacyPackages.${system})
        # mkPoetryApplication;
        # pkgs = nixpkgs.legacyPackages.${system};
        pkgs = nixpkgs.legacyPackages.x86_64-linux;

      in
        {
          # let
          # devEnv = poetry2nix.mkPoetryEnv {
          #     projectDir = ./.;
          # };

          packages = {
            piker = poetry2nix.mkPoetryEditablePackage {
            # env = poetry2nix.mkPoetryEnv {

              # NOTE: taken from surrounding inputs
              # projectDir = projectDir;
              editablePackageSources = { piker = ./piker; };

              # override msgspec to include setuptools as input
              # https://github.com/nix-community/poetry2nix/blob/master/docs/edgecases.md#modulenotfounderror-no-module-named-packagenamed
              overrides = poetry2nix.defaultPoetryOverrides.extend
                (self: super: {
                  msgspec = super.msgspec.overridePythonAttrs
                    (
                      old: {
                        buildInputs = (old.buildInputs or [ ]) ++ [ super.setuptools ];
                      }
                    );
                  }
                );

            };
        };


        # boot xonsh inside the poetry virtualenv when
        # define the custom entry point via an expected
        # output-attr that `nix-develop` scans for:
        # https://nixos.org/manual/nix/stable/command-ref/new-cli/nix3-develop.html#flake-output-attributes
        devShells.default = pkgs.mkShell {
          # packages = [ poetry2nix.packages.${system}.poetry ];
          packages = [ poetry2nix.packages.x86_64-linux.poetry ];
          shellHook = "poetry run xonsh";
          # shellHook = "poetry shell";
        };
      }
    );
}
