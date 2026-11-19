# Homebrew formula for pgwire-file (macOS + Linux): the pgwire-calcite runtime plus the file
# adapter's pack, the two release tarballs extracted into one root (runtimes/<version>/ and
# packs/file/<version>/; layout: pgwire-calcite/src/pgwire_calcite/packs.py). Another adapter's
# formula is this one with its pack's name. Package-manager delivery is not Gatekeeper-quarantined,
# so no codesigning or notarization is needed (PGW-031 revised).
#
# Replace VERSION and the sha256 values (each tarball's .sha256 asset) at release time.
class PgwireFile < Formula
  desc "PostgreSQL wire-protocol server over Apache Calcite's file adapter"
  homepage "https://github.com/kenstott/calcite"
  version "VERSION"
  license "BUSL-1.1"

  on_macos do
    on_arm do
      url "https://github.com/kenstott/calcite/releases/download/engine-v#{version}/pgwire-file-pack-#{version}-macos-arm64.tar.gz"
      sha256 "REPLACE_PACK_MACOS_ARM64_SHA256"
      resource "runtime" do
        url "https://github.com/kenstott/calcite/releases/download/engine-v#{version}/pgwire-calcite-runtime-#{version}-macos-arm64.tar.gz"
        sha256 "REPLACE_RUNTIME_MACOS_ARM64_SHA256"
      end
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/kenstott/calcite/releases/download/engine-v#{version}/pgwire-file-pack-#{version}-linux-x86_64.tar.gz"
      sha256 "REPLACE_PACK_LINUX_X86_64_SHA256"
      resource "runtime" do
        url "https://github.com/kenstott/calcite/releases/download/engine-v#{version}/pgwire-calcite-runtime-#{version}-linux-x86_64.tar.gz"
        sha256 "REPLACE_RUNTIME_LINUX_X86_64_SHA256"
      end
    end
    on_arm do
      url "https://github.com/kenstott/calcite/releases/download/engine-v#{version}/pgwire-file-pack-#{version}-linux-aarch64.tar.gz"
      sha256 "REPLACE_PACK_LINUX_AARCH64_SHA256"
      resource "runtime" do
        url "https://github.com/kenstott/calcite/releases/download/engine-v#{version}/pgwire-calcite-runtime-#{version}-linux-aarch64.tar.gz"
        sha256 "REPLACE_RUNTIME_LINUX_AARCH64_SHA256"
      end
    end
  end

  def install
    # Homebrew stages an archive inside its single top-level directory: packs/ here, runtimes/ in
    # the resource. Both go under libexec, so the pack's launcher finds runtimes/<version> beside it.
    (libexec/"packs").install Dir["*"]
    resource("runtime").stage { (libexec/"runtimes").install Dir["*"] }
    (bin/"pgwire-file").write <<~SH
      #!/bin/bash
      exec "#{libexec}/packs/file/#{version}/bin/pgwire-file" "$@"
    SH
    chmod 0755, bin/"pgwire-file"
  end

  test do
    assert_match "pgwire-calcite", shell_output("#{bin}/pgwire-file --help")
  end
end
