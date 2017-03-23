package esun.fbi.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhuhq on 2015/12/8.
 */
public class TestGangliaMetrics {
    @Test
    public void testGangliaReport() throws Exception{
        final MetricRegistry registry = new MetricRegistry();
        final Histogram randomNums = registry.histogram(MetricRegistry.name(TestGangliaMetrics.class, "random"));
        final GMetric ganglia = new GMetric("192.168.30.177", 8649, GMetric.UDPAddressingMode.UNICAST, 1);
        final GangliaReporter reporter = GangliaReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(ganglia);
        reporter.start(10, TimeUnit.SECONDS);
        Random random = new Random();
        while (true) {
            int randomInt = random.nextInt(1000);
            System.out.println("randomInt:" + randomInt);
            randomNums.update(randomInt);
            Thread.sleep(1000);
        }
    }

}
