/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.file.etl.ModelOperand;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link SenateEfdSession} against a stub helper that speaks the browser-helper protocol. */
@Tag("unit")
class SenateEfdSessionTest {

  @TempDir File dir;

  private void model(String script) {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("efdBrowserScript", script);
    operand.put("efdNodeCommand", "node");
    operand.put("efdEdgeExecutable", "/nonexistent/msedge.exe");
    operand.put("efdProfileDir", "C:\\Temp");
    operand.put("efdDebugPort", "9333");
    ModelOperand.capture("law", operand);
  }

  private String stub(String body) throws Exception {
    File script = new File(dir, "stub.js");
    Files.write(script.toPath(), body.getBytes(StandardCharsets.UTF_8));
    return script.getPath();
  }

  @Test void listingAndViewGoThroughTheHelper() throws Exception {
    model(stub("const rl = require('readline').createInterface({input: process.stdin});\n"
        + "rl.on('line', l => { const r = JSON.parse(l);\n"
        + " if (r.op === 'agree') console.log(JSON.stringify({ok: true}));\n"
        + " else if (r.op === 'post') console.log(JSON.stringify({status: 200, body: '{\"n\":'"
        + " + Object.keys(r.fields).length + '}'}));\n"
        + " else if (r.path === '/gone/') console.log(JSON.stringify({status: 200, redirected: true,"
        + " finalPath: '/search/home/', body: 'home'}));\n"
        + " else console.log(JSON.stringify({status: 200, redirected: false, finalPath: r.path,"
        + " body: '<html>view</html>'})); });\n"));
    SenateEfdSession session = new SenateEfdSession();
    Map<String, String> params = new HashMap<String, String>();
    params.put("a", "1");
    params.put("b", "2");
    assertEquals("{\"n\":2}", session.listing(params, false));
    SenateEfdSession.View view = session.getView("/search/view/annual/x-" + System.nanoTime() + "/");
    assertFalse(view.unavailable);
    assertEquals("<html>view</html>", view.body);
  }

  @Test void missingScriptFailsNamingTheCause() throws Exception {
    model(new File(dir, "absent.js").getPath());
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> new SenateEfdSession().acceptAgreement());
    assertTrue(e.getMessage().contains("script not found"), e.getMessage());
  }

  @Test void helperReportedErrorFailsWithItsCause() throws Exception {
    model(stub("console.log(JSON.stringify({error: 'Edge not found at /x'}));"));
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> new SenateEfdSession().acceptAgreement());
    assertTrue(e.getMessage().contains("Edge not found at /x"), e.getMessage());
  }
}
