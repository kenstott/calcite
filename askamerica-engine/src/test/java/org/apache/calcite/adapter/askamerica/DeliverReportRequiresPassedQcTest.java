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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The eval records an answer only after the product's own QC passed it. */
@Tag("unit")
@Isolated("REPORT_PASSED_QC is process-wide state")
class DeliverReportRequiresPassedQcTest {

    @AfterEach void reset() {
        McpServer.REPORT_PASSED_QC = false;
    }

    /** Never a valid run_subpath: deliverReport must reject it before writing anything, so the
     *  test cannot touch a real eval results directory. */
    private static final String NO_SUCH_RUN = "not-a-run-subpath";

    private static void deliver() throws Throwable {
        Method m = McpServer.class.getDeclaredMethod("deliverReport", String.class, String.class);
        m.setAccessible(true);
        try {
            m.invoke(null, NO_SUCH_RUN, "# draft");
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test void refusedWhenNoPreviewHasPassed() {
        McpServer.REPORT_PASSED_QC = false;
        Throwable t = assertThrows(Throwable.class, DeliverReportRequiresPassedQcTest::deliver);
        assertInstanceOf(IllegalArgumentException.class, t);
        assertTrue(t.getMessage().startsWith("deliver_report refused"));
    }

    @Test void notRefusedForQcOncePreviewPassed() {
        McpServer.REPORT_PASSED_QC = true;
        Throwable t = assertThrows(Throwable.class, DeliverReportRequiresPassedQcTest::deliver);
        // With QC passed it gets past the gate and fails on the invalid run_subpath instead.
        assertTrue(t.getMessage().startsWith("run_subpath must look like"), t.getMessage());
    }
}
