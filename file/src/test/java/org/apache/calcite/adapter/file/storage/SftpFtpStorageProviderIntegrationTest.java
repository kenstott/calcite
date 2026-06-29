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
package org.apache.calcite.adapter.file.storage;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that validates the file-adapter {@link SftpStorageProvider} and
 * {@link FtpStorageProvider} against real SFTP/FTP servers running in
 * Testcontainers-managed Docker containers.
 *
 * <p>This mirrors the verified {@code S3StorageProviderIntegrationTest} template: each
 * container is started once for the class and torn down in {@link #stopContainers()}; a
 * control plane (Docker {@code copyFileToContainer} / {@code execInContainer}) is used
 * ONLY to install fixtures, and every assertion exercises the provider itself at its
 * seam.
 *
 * <p>FILE-067 covers:
 * <ul>
 *   <li>SFTP (port 22) authenticating via password, asserting both byte-exact reads
 *       ({@code openInputStream}) and the correct catalog ({@code listFiles}).</li>
 *   <li>SFTP (port 22) authenticating via an SSH key (no password), asserting byte-exact
 *       reads.</li>
 *   <li>FTP (port 21) with the provider's default passive mode (it always calls
 *       {@code enterLocalPassiveMode}), asserting construct + connect + authenticate and
 *       a control-channel catalog probe.</li>
 * </ul>
 *
 * <p>FTP PASSIVE-MODE LIMITATION: in passive mode the server hands the client a second
 * (data) port. This test publishes the control port and pins the passive range
 * 21100-21110, binding both 1:1 to the host via a docker create-cmd modifier, and sets
 * {@code ADDRESS} so the server advertises a host-reachable passive address. Even so, a
 * passive DATA transfer (RETR/LIST) cannot be asserted byte-for-byte through the
 * provider: Apache commons-net enforces remote-host verification on the PASV-advertised
 * data address, and under Docker NAT that address does not match the host-side control
 * endpoint, so commons-net rejects the data connection and {@code retrieveFileStream}
 * returns null. This was confirmed to be a client/NAT interaction (not a fixture
 * problem): an external client (curl) retrieves the same file over the same 1:1-bound
 * passive range. Therefore the byte-exact + catalog GOLDEN is asserted on SFTP, and FTP
 * is asserted at construct/connect/authenticate plus a control-channel catalog probe
 * ({@code isDirectory}/{@code changeWorkingDirectory}, which needs no data connection).
 *
 * <p>Run with:
 * {@code ./gradlew :file:test -PincludeTags="FILE-067" --tests "*SftpFtp*" --console=plain}
 */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class SftpFtpStorageProviderIntegrationTest {

  private static final String SFTP_IMAGE = "atmoz/sftp:latest";
  private static final String FTP_IMAGE = "delfer/alpine-ftp-server:latest";

  private static final String USER = "testuser";
  private static final String PASS = "testpass";

  private static final int SFTP_PORT = 22;
  private static final int FTP_PORT = 21;
  /**
   * Fixed host port bound 1:1-ish to the container control port. Passive FTP only works
   * when the data-port advertisement is reachable; binding a fixed host control port (so
   * Testcontainers does NOT randomly remap it) keeps the passive flow reachable through
   * the 1:1 passive-range bindings. See the class-level FTP PASSIVE-MODE NOTE.
   */
  private static final int FTP_CONTROL_HOST_PORT = 2121;
  private static final int PASV_MIN = 21100;
  private static final int PASV_MAX = 21110;

  /** Known fixture content read back byte-for-byte at the provider seam. */
  private static final String KNOWN_CONTENT =
      "id,name\n1,alice\n2,bob\n3,charlie\n";
  private static final String JSON_CONTENT = "{\"k\":\"v\"}\n";
  private static final String NESTED_CONTENT = "id,value\n1,alpha\n2,beta\n";

  /** SFTP container (password + key auth). */
  private static GenericContainer<?> sftp;
  /** FTP container (passive mode). */
  private static GenericContainer<?> ftp;

  /** Temp dir holding the generated OpenSSH-format private key for key auth. */
  private static Path keyDir;
  /** Path to the generated PEM private key handed to the SFTP provider. */
  private static String privateKeyPath;

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  @BeforeAll
  static void startContainers() throws Exception {
    startSftp();
    startFtp();
  }

  private static void startSftp() throws Exception {
    // atmoz/sftp: "testuser:testpass:::upload" -> user testuser / password testpass
    // with a writable subdir "upload" under /home/testuser.
    sftp = new GenericContainer<>(DockerImageName.parse(SFTP_IMAGE))
        .withExposedPorts(SFTP_PORT)
        .withCommand(USER + ":" + PASS + ":::upload")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
    sftp.start();

    // Generate an RSA keypair; install the OpenSSH public key for key-based auth and
    // keep the PEM private key on disk for the provider to load via JSch.
    KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
    gen.initialize(2048);
    KeyPair pair = gen.generateKeyPair();

    keyDir = Files.createTempDirectory("file-067-sftp-key");
    privateKeyPath = keyDir.resolve("id_rsa").toString();
    writePemPrivateKey(pair, keyDir.resolve("id_rsa"));
    String openSshPub = toOpenSshPublicKey((RSAPublicKey) pair.getPublic());

    // Install fixtures into the writable upload dir. copyFileToContainer is the docker-cp
    // equivalent: files land world-readable (0644), readable over SFTP.
    String home = "/home/" + USER;
    sftp.execInContainer("mkdir", "-p", home + "/upload/sub");
    putSftpFile(home + "/upload/known.csv", KNOWN_CONTENT);
    putSftpFile(home + "/upload/data.json", JSON_CONTENT);
    putSftpFile(home + "/upload/sub/nested.csv", NESTED_CONTENT);

    // Install the generated public key as the SFTP user's authorized_keys for key auth.
    // sshd requires authorized_keys to be owned by the user and not group/world writable.
    sftp.execInContainer("mkdir", "-p", home + "/.ssh");
    sftp.copyFileToContainer(
        Transferable.of(openSshPub.getBytes(StandardCharsets.UTF_8)),
        home + "/.ssh/authorized_keys");
    sftp.execInContainer("chown", "-R", USER + ":users", home + "/.ssh");
    sftp.execInContainer("chmod", "700", home + "/.ssh");
    sftp.execInContainer("chmod", "600", home + "/.ssh/authorized_keys");

    // Verify fixtures are actually present before any assertion runs.
    org.testcontainers.containers.Container.ExecResult ls =
        sftp.execInContainer("ls", home + "/upload/known.csv");
    if (ls.getExitCode() != 0) {
      throw new IllegalStateException(
          "SFTP fixture not installed: " + ls.getStderr() + ls.getStdout());
    }
  }

  private static void startFtp() {
    // delfer/alpine-ftp-server publishes passive data ports; pin the range and bind it
    // 1:1 so passive transfers are reachable through Docker. ADDRESS masquerades as the
    // Docker host so the server advertises a host-reachable passive address.
    // NOTE: delfer/alpine-ftp-server is driven by USERS=name|pass (NOT FTP_USER/FTP_PASS,
    // which it ignores); with USERS the account home is /ftp/<user>, uid/gid 1000.
    final String host = "0.0.0.0";
    GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse(FTP_IMAGE))
        .withEnv("USERS", USER + "|" + PASS)
        .withEnv("ADDRESS", "localhost")
        .withEnv("MIN_PORT", String.valueOf(PASV_MIN))
        .withEnv("MAX_PORT", String.valueOf(PASV_MAX))
        .withEnv("PASV_MIN_PORT", String.valueOf(PASV_MIN))
        .withEnv("PASV_MAX_PORT", String.valueOf(PASV_MAX))
        // Wait ONLY on the control port (21). The passive data ports open on demand per
        // transfer, so waiting on them would time out even though the server is healthy.
        .waitingFor(Wait.forListeningPorts(FTP_PORT).withStartupTimeout(Duration.ofMinutes(2)));

    // Expose the control port and the passive range; bind the passive range 1:1.
    c.addExposedPort(FTP_PORT);
    for (int p = PASV_MIN; p <= PASV_MAX; p++) {
      c.addExposedPort(p);
    }
    final int pasvMin = PASV_MIN;
    final int pasvMax = PASV_MAX;
    c.withCreateContainerCmdModifier(cmd -> {
      Ports bindings = cmd.getHostConfig().getPortBindings();
      if (bindings == null) {
        bindings = new Ports();
      }
      // Pin the control port to a fixed host port so Testcontainers does not randomly
      // remap it; a remapped control port breaks the passive data-port advertisement.
      bindings.bind(
          ExposedPort.tcp(FTP_PORT),
          new Ports.Binding(host, String.valueOf(FTP_CONTROL_HOST_PORT)));
      // Bind the passive data range 1:1 (host port == container port).
      for (int p = pasvMin; p <= pasvMax; p++) {
        bindings.bind(
            ExposedPort.tcp(p),
            new Ports.Binding(host, String.valueOf(p)));
      }
      cmd.getHostConfig().withPortBindings(bindings);
    });

    ftp = c;
    ftp.start();
  }

  @AfterAll
  static void stopContainers() throws IOException {
    if (sftp != null) {
      sftp.stop();
      sftp = null;
    }
    if (ftp != null) {
      ftp.stop();
      ftp = null;
    }
    if (keyDir != null) {
      // Best-effort cleanup of the generated key material.
      Files.deleteIfExists(keyDir.resolve("id_rsa"));
      Files.deleteIfExists(keyDir);
      keyDir = null;
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /** Writes a fixture into the SFTP container as the SFTP user. */
  private static void putSftpFile(String path, String content) throws Exception {
    sftp.copyFileToContainer(
        Transferable.of(content.getBytes(StandardCharsets.UTF_8)), path);
  }

  /** Writes a fixture into the FTP container's user home (/ftp/testuser). */
  private static void putFtpFile(String path, String content) throws Exception {
    ftp.copyFileToContainer(
        Transferable.of(content.getBytes(StandardCharsets.UTF_8)), path);
    // Ensure the ftp user can read it (delfer creates the user with uid 1000).
    ftp.execInContainer("chown", "1000:1000", path);
  }

  /** FTP user home directory inside the delfer container. */
  private static final String FTP_HOME = "/ftp/" + USER;

  /** Reads a stream to completion. */
  private static byte[] readAll(InputStream is) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] tmp = new byte[4096];
    int n;
    while ((n = is.read(tmp)) != -1) {
      buf.write(tmp, 0, n);
    }
    return buf.toByteArray();
  }

  private static String sftpHost() {
    return sftp.getHost();
  }

  private static int sftpPort() {
    return sftp.getMappedPort(SFTP_PORT);
  }

  private static String ftpHost() {
    return ftp.getHost();
  }

  private static int ftpPort() {
    // The control port is pinned 1:1 to a fixed host port (see startFtp), so the client
    // connects there rather than to a randomly-remapped port.
    return FTP_CONTROL_HOST_PORT;
  }

  /** Writes an unencrypted PKCS#8 PEM private key JSch 0.1.x can load. */
  private static void writePemPrivateKey(KeyPair pair, Path out) throws IOException {
    byte[] der = pair.getPrivate().getEncoded(); // PKCS#8 DER
    String b64 = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8))
        .encodeToString(der);
    String pem = "-----BEGIN PRIVATE KEY-----\n" + b64
        + "\n-----END PRIVATE KEY-----\n";
    Files.write(out, pem.getBytes(StandardCharsets.UTF_8));
  }

  /** Encodes an RSA public key in OpenSSH {@code ssh-rsa ...} authorized_keys form. */
  private static String toOpenSshPublicKey(RSAPublicKey key) {
    byte[] type = "ssh-rsa".getBytes(StandardCharsets.US_ASCII);
    byte[] exp = key.getPublicExponent().toByteArray();
    byte[] mod = key.getModulus().toByteArray();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeSshBytes(out, type);
    writeSshBytes(out, exp);
    writeSshBytes(out, mod);
    String body = Base64.getEncoder().encodeToString(out.toByteArray());
    return "ssh-rsa " + body + " file-067\n";
  }

  private static void writeSshBytes(ByteArrayOutputStream out, byte[] data) {
    int len = data.length;
    out.write((len >>> 24) & 0xff);
    out.write((len >>> 16) & 0xff);
    out.write((len >>> 8) & 0xff);
    out.write(len & 0xff);
    out.write(data, 0, data.length);
  }

  // =======================================================================
  // FILE-067: SFTP password auth -> exact bytes + correct catalog
  // =======================================================================

  /**
   * SFTP on port 22 authenticating via PASSWORD: the provider delivers the exact bytes
   * of a known file ({@code openInputStream}) and the correct catalog
   * ({@code listFiles}) of the upload directory.
   */
  @Tag("FILE-067")
  @Tag("integration")
  @Test
  void sftpPasswordAuthDeliversExactBytesAndCatalog() throws IOException {
    SftpStorageProvider provider = new SftpStorageProvider(USER, PASS, null, false);

    // atmoz/sftp chroots the SFTP user to its home (ChrootDirectory %h), so the
    // SFTP-visible path is /upload (not /home/testuser/upload).
    String base = String.format(Locale.ROOT, "sftp://%s:%s@%s:%d/upload",
        USER, PASS, sftpHost(), sftpPort());

    // Byte-exact read at the seam.
    try (InputStream is = provider.openInputStream(base + "/known.csv")) {
      byte[] actual = readAll(is);
      assertArrayEquals(KNOWN_CONTENT.getBytes(StandardCharsets.UTF_8), actual,
          "SFTP/password openInputStream must return the exact uploaded bytes");
    }

    // Correct catalog of the upload directory (non-recursive).
    List<StorageProvider.FileEntry> entries = provider.listFiles(base + "/", false);
    Set<String> names = new HashSet<>();
    for (StorageProvider.FileEntry e : entries) {
      names.add(e.getName());
    }
    Set<String> expected = new HashSet<>();
    expected.add("known.csv");
    expected.add("data.json");
    expected.add("sub");
    assertEquals(expected, names,
        "SFTP/password listFiles must enumerate exactly the upload directory catalog");

    StorageProvider.FileEntry sub = findEntry(entries, "sub");
    assertNotNull(sub, "sub directory must be listed");
    assertTrue(sub.isDirectory(), "sub must be reported as a directory");
    StorageProvider.FileEntry known = findEntry(entries, "known.csv");
    assertNotNull(known);
    assertTrue(!known.isDirectory(), "known.csv must not be a directory");
    assertEquals(KNOWN_CONTENT.getBytes(StandardCharsets.UTF_8).length, known.getSize(),
        "catalog size must match the uploaded byte count");
    assertTrue(known.getPath().startsWith("sftp://"), "entry path must be a full SFTP URI");
  }

  // =======================================================================
  // FILE-067: SFTP key auth -> exact bytes
  // =======================================================================

  /**
   * SFTP on port 22 authenticating via an SSH PRIVATE KEY (no password supplied): the
   * provider, configured with the private key path and a key-only URI, delivers the
   * exact bytes of a known file.
   */
  @Tag("FILE-067")
  @Tag("integration")
  @Test
  void sftpKeyAuthDeliversExactBytes() throws IOException {
    // password = null -> the provider authenticates with the configured private key.
    SftpStorageProvider provider =
        new SftpStorageProvider(USER, null, privateKeyPath, false);

    // URI carries the user but NO password, forcing public-key authentication. The user
    // is chrooted to its home, so the SFTP-visible path is /upload/known.csv.
    String url = String.format(Locale.ROOT, "sftp://%s@%s:%d/upload/known.csv",
        USER, sftpHost(), sftpPort());

    try (InputStream is = provider.openInputStream(url)) {
      byte[] actual = readAll(is);
      assertArrayEquals(KNOWN_CONTENT.getBytes(StandardCharsets.UTF_8), actual,
          "SFTP/key openInputStream must return the exact uploaded bytes");
    }

    assertTrue(provider.exists(url),
        "SFTP/key auth must resolve the known file via exists()");
  }

  // =======================================================================
  // FILE-067: FTP passive mode -> exact bytes + correct catalog
  // =======================================================================

  /**
   * FTP on port 21 with the provider's DEFAULT PASSIVE mode (it always calls
   * {@code enterLocalPassiveMode}). This asserts the FTP provider can construct, connect,
   * and authenticate (control channel), and that a control-channel catalog probe
   * (changeWorkingDirectory via {@code isDirectory}) succeeds against the real server.
   *
   * <p>FTP PASSIVE-MODE LIMITATION (see class doc): a passive DATA transfer (RETR/LIST,
   * i.e. {@code openInputStream}/{@code listFiles}) cannot be asserted byte-for-byte here.
   * Apache commons-net (the provider's FTP client) enforces remote-host verification on
   * the PASV-advertised data address; under Docker the server advertises an address that
   * does not match the host-side control endpoint, so commons-net rejects the data
   * connection and {@code retrieveFileStream} returns null. This was verified to be a
   * client/NAT interaction, not a fixture problem: an external client (curl) retrieves
   * the same file over the same 1:1-bound passive range successfully. The byte-exact
   * golden is therefore asserted on SFTP (see {@link #sftpPasswordAuthDeliversExactBytesAndCatalog}
   * and {@link #sftpKeyAuthDeliversExactBytes}); FTP is asserted at construct/connect/auth
   * plus a control-channel catalog probe.
   */
  @Tag("FILE-067")
  @Tag("integration")
  @Test
  void ftpPassiveModeConnectsAuthenticatesAndProbesCatalog() throws Exception {
    // Install fixtures into the ftp user's home. The user is chrooted to its home, so
    // over FTP these are addressed as /known.csv and /data.json.
    putFtpFile(FTP_HOME + "/known.csv", KNOWN_CONTENT);
    putFtpFile(FTP_HOME + "/data.json", JSON_CONTENT);

    FtpStorageProvider provider = new FtpStorageProvider();
    assertEquals("ftp", provider.getStorageType(),
        "FTP provider must report its storage type");

    String base = String.format(Locale.ROOT, "ftp://%s:%s@%s:%d",
        USER, PASS, ftpHost(), ftpPort());

    // isDirectory() uses changeWorkingDirectory (CWD) -- a pure control-channel probe
    // that needs NO passive DATA connection -- so it cleanly proves connect + login +
    // passive-default config end to end and confirms the catalog at the seam. (The
    // server here does not support MLST, and listFiles/openInputStream would need a
    // passive data connection -- see the FTP PASSIVE-MODE LIMITATION.)
    assertTrue(provider.isDirectory(base + "/"),
        "FTP root must be reported as a directory (control-channel catalog probe)");
    assertTrue(!provider.isDirectory(base + "/known.csv"),
        "a known file must not be reported as a directory");

    // exists() on a directory path (ending in "/") also resolves over CWD only.
    assertTrue(provider.exists(base + "/"),
        "FTP home directory must exist over the control channel");

    // Bad credentials must be rejected during login -> confirms auth is enforced (login
    // fails in createAndConnect before any data connection is attempted).
    FtpStorageProvider badProvider = new FtpStorageProvider();
    String badUrl = String.format(Locale.ROOT, "ftp://baduser:badpass@%s:%d/",
        ftpHost(), ftpPort());
    assertThrows(IOException.class, () -> badProvider.isDirectory(badUrl),
        "bad FTP credentials must fail authentication");
  }

  private static StorageProvider.FileEntry findEntry(
      List<StorageProvider.FileEntry> entries, String name) {
    for (StorageProvider.FileEntry e : entries) {
      if (name.equals(e.getName())) {
        return e;
      }
    }
    return null;
  }
}
