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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * REQ-1992: every file the adapter writes for a schema goes under the base directory the model
 * names. The model file's own directory, which Calcite fills into the {@code baseDirectory}
 * operand when the model sets none, is not a configured location.
 */
@Tag("unit")
class BaseDirectoryRequirementsTest {

  @TempDir Path dir;

  private Path model(String operand) throws Exception {
    Path data = Files.createDirectories(dir.resolve("model").resolve("data"));
    Files.write(data.resolve("people.csv"), "id,name\n1,alice\n2,bob\n"
        .getBytes(StandardCharsets.UTF_8));
    Path model = dir.resolve("model").resolve("model.json");
    Files.write(model, ("{\"version\": \"1.0\", \"defaultSchema\": \"FILES\", \"schemas\": [{"
        + "\"name\": \"FILES\", \"type\": \"custom\","
        + "\"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\": {" + operand + "}}]}").getBytes(StandardCharsets.UTF_8));
    return model;
  }

  private static Connection connect(Path model) throws SQLException {
    return DriverManager.getConnection("jdbc:calcite:model=" + model.toAbsolutePath()
        + ";lex=JAVA");
  }

  @Test void aModelFileSchemaWithoutABaseDirectoryIsRefusedNamingIt() throws Exception {
    Path model = model("\"directory\": \"data\", \"executionEngine\": \"linq4j\"");
    // Calcite wraps the factory's refusal ("Error instantiating JsonCustomSchema")
    Exception e = assertThrows(Exception.class, () -> connect(model).close());
    Throwable cause = e;
    while (cause.getCause() != null && !(cause instanceof IllegalArgumentException)) {
      cause = cause.getCause();
    }
    assertTrue(cause.getMessage().contains("File schema 'FILES' has no baseDirectory"),
        cause.getMessage());
    // and nothing was written beside the model
    assertFalse(new File(dir.resolve("model").toFile(), ".aperio").exists());
  }

  @Test void aRelativeBaseResolvesAgainstTheModelAndSoDoesTheDirectory() throws Exception {
    Path model = model("\"directory\": \"data\", \"baseDirectory\": \"../written\","
        + " \"executionEngine\": \"linq4j\"");
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT count(*) FROM FILES.people")) {
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
    }
    assertTrue(new File(dir.toFile(), "written/.aperio").isDirectory(),
        "the schema's files are under the base directory the model named");
    assertFalse(new File(dir.resolve("model").toFile(), ".aperio").exists());
  }
}
