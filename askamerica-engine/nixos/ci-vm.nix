# The host of host.nix as a QEMU guest on a GitHub-hosted runner. Everything here is the test's
# own scaffolding (a user, sshd, a psql client, the VM's shape); none of it is something
# AskAmerica needs from a host. What AskAmerica needs is host.nix, and only that.
{ pkgs, ... }:
{
  system.stateVersion = "26.05";
  networking.hostName = "askamerica-nixos";
  time.timeZone = "UTC";

  # Never booted from: the VM build replaces both. A NixOS system must declare them to evaluate.
  fileSystems."/" = {
    device = "/dev/disk/by-label/nixos";
    fsType = "ext4";
  };
  boot.loader.grub.device = "nodev";

  # The workflow drives the guest over ssh with a key it generates for the run and writes beside
  # this file before the build.
  users.users.tester = {
    isNormalUser = true;
    openssh.authorizedKeys.keyFiles = [ ./ci_authorized_key.pub ];
  };
  services.openssh = {
    enable = true;
    # The data-path step hands the object store's credentials to one ssh session through its
    # environment; nothing else is accepted.
    extraConfig = "AcceptEnv AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_OVERRIDE GOVDATA_PARQUET_DIR";
  };

  # The test's client: it queries the server over the PostgreSQL wire protocol.
  environment.systemPackages = [ pkgs.postgresql ];

  virtualisation.vmVariant.virtualisation = {
    cores = 4;
    memorySize = 10240;
    # MB: two unpacked bundles (about 2.5 GB) and their state.
    diskSize = 20480;
    graphics = false;
    forwardPorts = [
      {
        from = "host";
        host.port = 2222;
        guest.port = 22;
      }
    ];
    # The bundles built by this run and the proof script, read by the guest.
    sharedDirectories.bundles = {
      source = ''"$ASKAMERICA_BUNDLES"'';
      target = "/mnt/bundles";
    };
  };
}
