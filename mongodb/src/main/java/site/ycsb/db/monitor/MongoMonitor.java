package site.ycsb.db.monitor;

import com.mongodb.MongoClientSettings;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;

/**
 * @author liujinyong
 * @date 2021/11/19 1:06 下午
 */
public final class MongoMonitor {
  private MongoMonitor() {
  }

  public static MongoClientSettings.Builder mongoClientSettingsBuilder(int monitorPort) {
    PrometheusConfig prometheusConfig = new PrometheusConfig() {
      @Override
      public String get(String key) {
        return null;
      }
    };
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(prometheusConfig);
    registry.config().commonTags("clusterName", "mongoTest", "instanceName", "monitorPort:" + monitorPort);

    try {
      HTTPServer server = new HTTPServer.Builder()
          .withPort(monitorPort)
          .withRegistry(registry.getPrometheusRegistry())
          .build();
    } catch (IOException e) {
      System.err.println(e);
    }


    return MongoClientSettings.builder()
        .addCommandListener(new MongoMetricsCommandListener(registry))
        .applyToConnectionPoolSettings((block) -> block.addConnectionPoolListener(
            new MongoMetricsConnectionPoolListener(registry)));
  }
}
