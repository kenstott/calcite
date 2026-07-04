# Homebrew formula for pgwire-calcite (macOS + Linux).
#
# Package-manager delivery avoids Gatekeeper quarantine, so NO codesigning/
# notarization is required for this path (PGW-031 revised). Publish this in a tap
# (e.g. kenstott/homebrew-tap) and: `brew install kenstott/tap/pgwire-calcite`.
#
# Replace VERSION and the sha256 values (from pack.sh output) at release time.
class PgwireCalcite < Formula
  desc "PostgreSQL wire-protocol server backed by Apache Calcite"
  homepage "https://github.com/kenstott/calcite"
  version "VERSION"
  license "BUSL-1.1"

  on_macos do
    on_arm do
      url "https://github.com/kenstott/calcite/releases/download/pgwire-v#{version}/pgwire-calcite-#{version}-macos-arm64.tar.gz"
      sha256 "REPLACE_MACOS_ARM64_SHA256"
    end
    on_intel do
      url "https://github.com/kenstott/calcite/releases/download/pgwire-v#{version}/pgwire-calcite-#{version}-macos-x86_64.tar.gz"
      sha256 "REPLACE_MACOS_X86_64_SHA256"
    end
  end

  on_linux do
    url "https://github.com/kenstott/calcite/releases/download/pgwire-v#{version}/pgwire-calcite-#{version}-linux-x86_64.tar.gz"
    sha256 "REPLACE_LINUX_X86_64_SHA256"
  end

  def install
    # The tarball is the self-contained airgap bundle (jre/ cpython/ wheelhouse/
    # jars/ bin/). Install under libexec and symlink the launcher.
    libexec.install Dir["*"]
    (bin/"pgwire-calcite").write <<~SH
      #!/bin/bash
      exec "#{libexec}/bin/pgwire-calcite" "$@"
    SH
    chmod 0755, bin/"pgwire-calcite"
  end

  test do
    assert_match "pgwire-calcite", shell_output("#{bin}/pgwire-calcite --help")
  end
end
