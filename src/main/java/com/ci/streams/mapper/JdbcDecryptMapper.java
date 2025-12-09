package com.ci.streams.mapper;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;

/** JDBC를 사용하여 암호화된 데이터를 복호화하는 매퍼 추상 클래스. HikariCP를 사용하여 DB 연결을 관리합니다. */
public abstract class JdbcDecryptMapper extends AbstractJsonDebeziumMapper {

  private static final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

  protected JdbcDecryptMapper(Schema outputSchema, Map<String, Object> params) {
    super(outputSchema, params);
    initDataSource();
  }

  private synchronized void initDataSource() {
    if (!dataSources.containsKey("DI")) {
      try {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        String url = dotenv.get("JDBC_URL");
        String username = dotenv.get("JDBC_USERNAME");
        String password = dotenv.get("JDBC_PASSWORD");
        String driver = dotenv.get("JDBC_DRIVER", "oracle.jdbc.OracleDriver");

        if (url != null && username != null && password != null) {
          HikariConfig config = new HikariConfig();
          config.setJdbcUrl(url);
          config.setUsername(username);
          config.setPassword(password);
          config.setDriverClassName(driver);

          config.setMaximumPoolSize(10);
          config.setMinimumIdle(2);
          config.setIdleTimeout(30000);
          config.setConnectionTimeout(30000);

          HikariDataSource ds = new HikariDataSource(config);
          dataSources.put("DI", ds);
          log.info("HikariCP DI DataSource initialized.");
        } else {
          log.warn("JDBC configuration missing in .env. Database features will be disabled.");
        }
      } catch (Exception e) {
        log.error("Failed to initialize DataSource", e);
      }
    }
  }

  protected String decryptField(
      String dbKey, String tableName, String targetCol, String pkCol, String pkValue) {
    return decryptField(dbKey, tableName, targetCol, Map.of(pkCol, pkValue));
  }

  protected String decryptField(
      String dbKey, String tableName, String targetCol, Map<String, String> pkMap) {
    HikariDataSource ds = dataSources.get(dbKey);
    if (ds == null) return null;
    if (pkMap == null || pkMap.isEmpty()) return null;

    StringBuilder whereClause = new StringBuilder();
    List<Object> params = new ArrayList<>();

    int i = 0;
    for (Map.Entry<String, String> entry : pkMap.entrySet()) {
      if (i > 0) whereClause.append(" AND ");
      whereClause.append(entry.getKey()).append(" = ?");
      params.add(entry.getValue());
      i++;
    }

    String sql =
        String.format("SELECT %s FROM %s WHERE %s", targetCol, tableName, whereClause.toString());

    try (Connection conn = ds.getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {

      for (int j = 0; j < params.size(); j++) {
        pstmt.setObject(j + 1, params.get(j));
      }

      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString(1);
        }
      }
    } catch (Exception e) {
      log.error(
          "Failed to decrypt field {} from table {} for keys {} (DB: {})",
          targetCol,
          tableName,
          pkMap,
          dbKey,
          e);
    }
    return null;
  }
}
