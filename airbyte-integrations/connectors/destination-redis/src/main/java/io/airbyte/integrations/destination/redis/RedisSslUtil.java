package io.airbyte.integrations.destination.redis;

import static io.airbyte.integrations.destination.redis.RedisSslUtil.SslMode.VERIFY_IDENTITY;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSslUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisSslUtil.class);

  public static final String PARAM_CLIENT_CERTIFICATE = "client_certificate";
  public static final String PARAM_CLIENT_KEY = "client_key";
  public static final String PARAM_CA_CERTIFICATE = "ca_certificate";
  public static final String PARAM_CLIENT_KEY_PASSWORD = "client_key_password";
  public static final String PARAM_SSL_MODE = "mode";
  public static final String PARAM_SSL_MODE_VERIFY_FULL = "verify-full";
  public static final String PARAM_SSL = "ssl";

  private static final String CLIENT_CERTIFICATE = "client.crt";
  private static final String CLIENT_CA_CERTIFICATE = "client-ca.crt";
  private static final String CLIENT_KEY = "client.key";
  private static final String CLIENT_KEY_STORE = "client_key_store.p12";
  private static final String KEY_STORE_TYPE = "PKCS12";
  private static final String TRUST_STORE = "truststore.jks";
  private static final String TRUST_PASSWORD = "truststore_pwd";
  private static final String TRUST_TYPE = "JKS";

  /**
   * set javax.net.ssl.keyStore and javax.net.ssl.trustStore based on provided ca.crt, client.crt, client.kay
   *
   * @param redisSslMode json ssl mode config
   */
  public static void setupCertificates(final JsonNode redisSslMode) {
    try {
      if (getSslVerifyMode(redisSslMode) == VERIFY_IDENTITY) {
        LOGGER.info("Preparing ssl certificates for {} mode", PARAM_SSL_MODE_VERIFY_FULL);
        final String clientKeyPassword = getOrGeneratePassword(redisSslMode);
        initCertificateStores(redisSslMode.get(PARAM_CA_CERTIFICATE).asText(),
            redisSslMode.get(PARAM_CLIENT_CERTIFICATE).asText(), redisSslMode.get(PARAM_CLIENT_KEY).asText(), clientKeyPassword);
      }
    } catch (final IOException | InterruptedException e) {
      throw new RuntimeException("Failed to import certificate into Java Keystore");
    }
  }

  /**
   * Generate random pass if key pass param is empty
   *
   * @param sslModeConfig json ssl mode config
   * @return client key password
   */
  private static String getOrGeneratePassword(final JsonNode sslModeConfig) {
    final String clientKeyPassword;
    if (sslModeConfig.has(PARAM_CLIENT_KEY_PASSWORD) && !sslModeConfig.get(PARAM_CLIENT_KEY_PASSWORD).asText().isEmpty()) {
      clientKeyPassword = sslModeConfig.get(PARAM_CLIENT_KEY_PASSWORD).asText();
    } else {
      clientKeyPassword = RandomStringUtils.randomAlphanumeric(10);
    }
    return clientKeyPassword;
  }

  /**
   * The method generate certificates based on provided ca.crt, client.crt, client.kay.
   * Generated keys
   *
   * @param caCertificate certificate to validate client certificate and key.
   * @param clientCertificate The client certificate.
   * @param clientKey The client key.
   * @param clientKeyPassword The client key password.
   */
  private static void initCertificateStores(
      final String caCertificate,
      final String clientCertificate,
      final String clientKey,
      final String clientKeyPassword)
      throws IOException, InterruptedException {

    LOGGER.info("Try to generate {}", CLIENT_KEY_STORE);
    createCertificateFile(CLIENT_CERTIFICATE, clientCertificate);
    createCertificateFile(CLIENT_KEY, clientKey);
    runProcess("openssl pkcs12 -export -in " + CLIENT_CERTIFICATE + " -inkey " + CLIENT_KEY + " -out " + CLIENT_KEY_STORE + " -passout pass:"
        + clientKeyPassword + "", Runtime.getRuntime());
    LOGGER.info("{} Generated", CLIENT_KEY_STORE);

    LOGGER.info("Try to generate {}", TRUST_STORE);
    createCertificateFile(CLIENT_CA_CERTIFICATE, caCertificate);
    runProcess("keytool -import -file " + CLIENT_CA_CERTIFICATE + " -alias redis-ca -keystore " + TRUST_STORE + " -storepass " + TRUST_PASSWORD
        + "  -noprompt", Runtime.getRuntime());
    LOGGER.info("{} Generated", TRUST_STORE);

    setSystemProperty(clientKeyPassword);
  }

  private static void runProcess(final String cmd, final Runtime run) throws IOException, InterruptedException {
    final Process pr = run.exec(cmd);
    if (!pr.waitFor(30, TimeUnit.SECONDS)) {
      pr.destroy();
      throw new RuntimeException("Timeout while executing: " + cmd);
    }
  }

  private static void createCertificateFile(final String fileName, final String fileValue) throws IOException {
    try (final PrintWriter out = new PrintWriter(fileName, StandardCharsets.UTF_8)) {
      out.print(fileValue);
    }
  }

  private static void setSystemProperty(final String clientKeyPassword) {
    System.setProperty("javax.net.ssl.keyStoreType", KEY_STORE_TYPE);
    System.setProperty("javax.net.ssl.keyStore", CLIENT_KEY_STORE);
    System.setProperty("javax.net.ssl.keyStorePassword", clientKeyPassword);
    System.setProperty("javax.net.ssl.trustStoreType", TRUST_TYPE);
    System.setProperty("javax.net.ssl.trustStore", TRUST_STORE);
    System.setProperty("javax.net.ssl.trustStorePassword", TRUST_PASSWORD);
  }

  public static boolean isSsl(JsonNode jsonConfig) {
    return jsonConfig.has(PARAM_SSL) && jsonConfig.get(PARAM_SSL).asBoolean();
  }

  private static SslMode getSslVerifyMode(JsonNode sslModeParam) {
    return SslMode.bySpec(sslModeParam.get(PARAM_SSL_MODE).asText()).orElseThrow(() -> new IllegalArgumentException("unexpected ssl mode"));
  }

  public enum SslMode {

    DISABLED("disable"),
    VERIFY_IDENTITY( "verify-full");

    public final List<String> spec;

    SslMode(final String... spec) {
      this.spec = Arrays.asList(spec);
    }

    public static Optional<SslMode> bySpec(final String spec) {
      return Arrays.stream(SslMode.values())
          .filter(sslMode -> sslMode.spec.contains(spec))
          .findFirst();
    }

  }


}
