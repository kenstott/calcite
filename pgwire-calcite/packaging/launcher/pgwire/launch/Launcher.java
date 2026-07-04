// Copyright (c) 2026 Kenneth Stott
// Licensed under the Business Source License 1.1 (see repo LICENSE/NOTICE).
//
// OS-agnostic Java entry point (PGW-025). The shipped app is launched as a Java
// application (java -jar pgwire-launcher.jar …); this thin launcher locates the
// bundled standalone CPython and starts the pgwire-calcite Python launcher (which
// in turn runs the supervisor / pgwire + Calcite children). Keeping supervision in
// Python (supervisor.py) while the top-level entry is Java satisfies the
// "OS-agnostic Java application, provisions CPython" requirement without a second
// supervisor implementation.
package pgwire.launch;

import java.util.ArrayList;
import java.util.List;

public final class Launcher {
  private Launcher() {}

  public static void main(String[] args) throws Exception {
    // Bundled CPython (set by the installer/bundle); falls back to PATH python3.
    String python = System.getProperty("pgwire.python",
        System.getenv().getOrDefault("PGWIRE_PYTHON", "python3"));
    String module = System.getProperty("pgwire.module", "pgwire_calcite.launcher");

    List<String> cmd = new ArrayList<>();
    cmd.add(python);
    cmd.add("-m");
    cmd.add(module);
    for (String a : args) {
      cmd.add(a);
    }

    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.inheritIO();
    String home = System.getProperty("pgwire.home");
    if (home != null) {
      pb.environment().put("PGWIRE_HOME", home);
    }
    Process p = pb.start();
    Runtime.getRuntime().addShutdownHook(new Thread(p::destroy));
    System.exit(p.waitFor());
  }
}
