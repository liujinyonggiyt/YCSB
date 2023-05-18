package site.ycsb.measurements.monitor;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static site.ycsb.Client.LABEL_PROPERTY;

/**
 * @author liujinyong
 * @create 2023/5/15 4:53 PM
 */
public final class MonitorManager {
  public static final String MONITOR_ENABLE = "monitor.enable";
  public static final String MONITOR_PATH = "monitor.path";
  public static final String MONITOR_PORT = "monitor.port";
  private static final MonitorManager INSTANCE = new MonitorManager();
  private boolean enable;
  @Nullable
  private HttpServer httpServer;
  @Nullable
  private PrometheusMeterRegistry registry;
  private Map<String, Timer> timerMap = new ConcurrentHashMap<>();
  private Map<String, Counter> batchSizeMap = new ConcurrentHashMap<>();
  private MonitorManager() {
  }

  public static MonitorManager getInstance(){
    return INSTANCE;
  }

  public void init(Properties p) throws IOException {
    enable = Boolean.parseBoolean(p.getProperty(MONITOR_ENABLE, "true"));
    if(enable){
      String monitorPath = p.getProperty(MONITOR_PATH, "/prometheus");
      int monitorPort = Integer.parseInt(p.getProperty(MONITOR_PORT, "80"));
      String label = p.getProperty(LABEL_PROPERTY, "");
      PrometheusConfig prometheusConfig = new PrometheusConfig() {
        @Override
        public String get(String key) {
          return null;
        }
      };
      registry = new PrometheusMeterRegistry(prometheusConfig);
      if(!label.isEmpty()){
        registry.config().commonTags("label", label);
      }

      {
        HttpServer tempHttpServer = HttpServer.create(new InetSocketAddress(monitorPort), 0);
        tempHttpServer.createContext(monitorPath, httpExchange -> {
          String response = registry.scrape();
          httpExchange.sendResponseHeaders(200, response.getBytes().length);
          try (OutputStream os = httpExchange.getResponseBody()) {
            os.write(response.getBytes());
          }
        });
        new Thread(tempHttpServer::start).start();

        httpServer = tempHttpServer;
      }

    }
  }

  public void cleanup(){
    if(null!=httpServer){
      httpServer.stop(10);
    }
  }

  public void monitorOpt(String optName, long costTime, TimeUnit timeUnit){
    if(!enable){
      return;
    }
    timerMap.computeIfAbsent(optName, new Function<String, Timer>() {
      @Override
      public Timer apply(String s) {
        return Timer.builder("optInvoke").tags("optName", optName)
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofNanos(1000))
            .maximumExpectedValue(Duration.ofMillis(100))
            .register(registry);
      }
    }).record(costTime, timeUnit);
  }
  public void batchOpt(String optName, int batchSize){
    if(!enable){
      return;
    }
    batchSizeMap.computeIfAbsent(optName, new Function<String, Counter>() {
      @Override
      public Counter apply(String s) {
        return Counter.builder("batchOptInvoke").tags("optName", optName)
            .register(registry);
      }
    }).increment(batchSize);
  }

}
