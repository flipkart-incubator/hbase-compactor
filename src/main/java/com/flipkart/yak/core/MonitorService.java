package com.flipkart.yak.core;


import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.flipkart.yak.config.CompactionContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class MonitorService {

    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final ConcurrentHashMap <String, Metric> metricStore = new ConcurrentHashMap();
    private static final String DELIMITER = "_";
    private static final String METRIC_TYPE_DELIMITER = ".";

    private enum MetricType {
        METER, COUNTER, TIMER;
    };
    public static  void reportCounterIncr(Class name, CompactionContext  context, String metric, int value) {
        Counter counter = (Counter)getMetric(name, context,metric, MetricType.COUNTER);
        counter.inc(value);
    }

    public static  void reportCounterDecr(Class name, CompactionContext  context, String metric, int value) {
        Counter counter = (Counter)getMetric(name, context, metric, MetricType.COUNTER);
        counter.dec(value);
    }

    public static void reportValue(Class name, CompactionContext  context, String metric, int value) {
        Meter meter = (Meter) getMetric(name, context,metric, MetricType.METER);
        meter.mark(value);
    }

    private static String createMetricName(Class source, CompactionContext  context, String name) {
        String sourceName = source.getCanonicalName();
        String groupName = context.getRsGroup();
        return sourceName+DELIMITER+groupName+DELIMITER+name;
    }

    private static Metric getMetric(Class source, CompactionContext  context, String name, MetricType metric){
        String metricName = createMetricName(source,context, name);
        String metricNameForStorage = metricName + METRIC_TYPE_DELIMITER + metric;
        if ( metric.equals(MetricType.METER)){
            metricStore.putIfAbsent(metricNameForStorage, metricRegistry.meter(metricName));
        }
        if ( metric.equals(MetricType.TIMER)){
            metricStore.putIfAbsent(metricNameForStorage, metricRegistry.timer(metricName));
        }
        if ( metric.equals(MetricType.COUNTER)){
            metricStore.putIfAbsent(metricNameForStorage, metricRegistry.counter(metricName));
        }
        return metricStore.get(metricNameForStorage);
    }

    public static void start() {
        JmxReporter reporter = JmxReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start();

    }
}
