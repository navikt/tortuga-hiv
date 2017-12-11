package no.nav.opptjening.hoi;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.hotspot.DefaultExports;
import io.prometheus.client.spring.boot.EnablePrometheusEndpoint;
import io.prometheus.client.spring.boot.EnableSpringBootMetricsCollector;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.StopWatch;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
@EnablePrometheusEndpoint
@EnableSpringBootMetricsCollector
@EnableScheduling
public class Application {

    private static final StopWatch startupStopWatch = new StopWatch();

    public static void main(String [] args) {
        startupStopWatch.start();
        SpringApplication.run(Application.class, args);
    }

    @EventListener
    public void registerJvmMetricsOnBoot(ApplicationReadyEvent event) {
        DefaultExports.initialize();
    }

    @EventListener
    public void onApplicationStartup(ApplicationReadyEvent event) {
        startupStopWatch.stop();

        new StartupInfoExports( ManagementFactory.getRuntimeMXBean().getUptime() / 1000.0, startupStopWatch.getTotalTimeSeconds()).register();
    }

    private static class StartupInfoExports extends Collector {
        private final double jvmStartupTime;
        private final double appStartupTime;

        private StartupInfoExports(double jvmStartupTime, double appStartupTime) {
            this.jvmStartupTime = jvmStartupTime;
            this.appStartupTime = appStartupTime;
        }

        public List<MetricFamilySamples> collect() {
            List<MetricFamilySamples> mfs = new ArrayList<>();

            GaugeMetricFamily startupInfo = new GaugeMetricFamily("startup_time", "How long time startup took", Arrays.asList("type"));
            startupInfo.addMetric(Arrays.asList("jvm"), jvmStartupTime);
            startupInfo.addMetric(Arrays.asList("app"), appStartupTime);

            mfs.add(startupInfo);

            return mfs;
        }
    }
}
