{
  description = "AskAmerica on NixOS: the host module, and the guest the release workflow tests it in";

  # nixos-26.05 at a fixed commit, so every run builds the same system; move it deliberately.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/7c8764b7c7b09b34f632464276218ef9090eaa11";

  outputs =
    { nixpkgs, ... }:
    {
      # What a host must provide. Reusable on any NixOS machine.
      nixosModules.askamerica = ./host.nix;

      # That host as a QEMU guest, for the `nixos` job of pgwire-adapters-release.yml:
      #   nix build path:askamerica-engine/nixos#nixosConfigurations.ci.config.system.build.vm
      nixosConfigurations.ci = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = [
          ./host.nix
          ./ci-vm.nix
        ];
      };
    };
}
