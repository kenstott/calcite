# What a NixOS host must provide for AskAmerica's pg-wire server bundle (requirement ASKAM-001).
#
# Import this module, or copy its two settings into your configuration:
#
#   imports = [ ./host.nix ];
#
# The bundle carries its own Java and Python runtimes and its own native libraries' loaders
# are the generic Linux ones, which NixOS does not have at their usual paths. nix-ld supplies
# them. Nothing else is needed: no JDK, no JAVA_HOME, no Python. bash, coreutils and tar, which
# the launcher and the installer call, are part of every NixOS system.
#
# This file is the configuration the release is tested against: the `nixos` job of
# .github/workflows/pgwire-adapters-release.yml boots a guest with this module and nothing
# else of substance, unpacks the bundle read-only, starts the server and queries it.
#
# A process started outside a login shell (a systemd service) does not inherit the login
# environment nix-ld is configured through; start the bundle's launcher from a login shell, or
# give the service that environment.
{ pkgs, ... }:
{
  programs.nix-ld = {
    enable = true;
    # What the bundled JRE, the bundled CPython and their native modules load from the host.
    # A library the release test finds missing is added here, with the name of what needed it.
    libraries = with pkgs; [
      stdenv.cc.cc.lib
      zlib
    ];
  };
}
