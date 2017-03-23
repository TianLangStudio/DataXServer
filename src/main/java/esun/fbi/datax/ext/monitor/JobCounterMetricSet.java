package esun.fbi.datax.ext.monitor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhuhq on 2015/12/9.
 */
public class JobCounterMetricSet implements MetricSet {
    private final Map<String,Metric> jobCounters = new HashMap<String, Metric>();
    @Override
    public Map<String, Metric> getMetrics() {
        return jobCounters;
    }
    public void put(String name,Counter counter) {
        jobCounters.put(name,counter);
    }
}
