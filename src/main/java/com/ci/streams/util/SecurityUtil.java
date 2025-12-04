package com.ci.streams.util;

import java.io.StringReader;
import java.io.StringWriter;
import java.security.PrivateKey;
import java.util.Properties;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SecurityUtil {
  private static final Logger log = LoggerFactory.getLogger(SecurityUtil.class);

  private SecurityUtil() {}

  /** Kafka 속성에 보안 설정을 적용합니다. PEM 인증서 및 키가 존재하면 SSL 설정을 구성합니다. */
  public static void applySecurity(final Properties properties) {
    final String pemTrust = Env.readEnvOrFile("KAFKA_TRUSTED_CERT");
    final String pemCert = Env.readEnvOrFile("KAFKA_CLIENT_CERT");
    final String pemKey = Env.readEnvOrFile("KAFKA_CLIENT_CERT_KEY");

    if (Env.notBlank(pemTrust) && Env.notBlank(pemCert) && Env.notBlank(pemKey)) {
      if (log.isInfoEnabled()) {
        log.info("PEM configuration detected. Using Native Kafka PEM support.");
      }

      properties.put("security.protocol", "SSL");
      properties.put("ssl.truststore.type", "PEM");
      properties.put("ssl.truststore.certificates", normalizePem(pemTrust));
      properties.put("ssl.keystore.type", "PEM");
      properties.put("ssl.keystore.certificate.chain", normalizePem(pemCert));
      properties.put("ssl.keystore.key", ensurePkcs8(normalizePem(pemKey)));
      properties.put("ssl.endpoint.identification.algorithm", "");
    }
  }

  public static String normalizePem(final String pem) {
    if (pem == null) {
      return null;
    }
    String tempPem = pem.replace("\\n", "\n");
    tempPem = tempPem.replace("\r\n", "\n");
    tempPem = tempPem.replace("\r", "\n");
    tempPem = tempPem.trim();

    if (!tempPem.isEmpty() && tempPem.charAt(0) == '\uFEFF') {
      tempPem = tempPem.substring(1);
    }
    return tempPem;
  }

  private static String ensurePkcs8(String pemKey) {
    if (pemKey == null) return null;

    if (pemKey.contains("BEGIN PRIVATE KEY")) {
      return pemKey;
    }

    try (PEMParser pemParser = new PEMParser(new StringReader(pemKey))) {
      Object object = pemParser.readObject();
      PrivateKeyInfo pkInfo;

      if (object instanceof PEMKeyPair) {
        PEMKeyPair pemKeyPair = (PEMKeyPair) object;
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        PrivateKey privateKey = converter.getPrivateKey(pemKeyPair.getPrivateKeyInfo());

        StringWriter writer = new StringWriter();
        try (PemWriter pemWriter = new PemWriter(writer)) {
          JcaPKCS8Generator gen = new JcaPKCS8Generator(privateKey, null);
          pemWriter.writeObject(gen.generate());
        }
        return writer.toString();

      } else if (object instanceof PrivateKeyInfo) {
        pkInfo = (PrivateKeyInfo) object;
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        PrivateKey privateKey = converter.getPrivateKey(pkInfo);

        StringWriter writer = new StringWriter();
        try (PemWriter pemWriter = new PemWriter(writer)) {
          JcaPKCS8Generator gen = new JcaPKCS8Generator(privateKey, null);
          pemWriter.writeObject(gen.generate());
        }
        return writer.toString();
      } else {
        if (log.isWarnEnabled()) {
          log.warn(
              "Unknown PEM object type: {}. Returning original key.", object.getClass().getName());
        }
        return pemKey;
      }
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Failed to convert PEM key to PKCS#8. Returning original key.", e);
      }
      return pemKey;
    }
  }
}
