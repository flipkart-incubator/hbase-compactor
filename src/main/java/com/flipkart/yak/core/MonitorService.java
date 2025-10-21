package com.flipkart.yak.core;


import com.codahale.metrics.*;
import com.codahale.metrics.jmx.JmxReporter;
import com.flipkart.yak.config.CompactionContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class MonitorService {

    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final ConcurrentHashMap <String, Metric> metricStore = new ConcurrentHashMap();
    private static final ConcurrentHashMap<String, AtomicInteger> gaugeAtomics = new ConcurrentHashMap<>();
    private static final String DELIMITER = "_";
    private static final String METRIC_TYPE_DELIMITER = ".";

    private enum MetricType {
        METER, COUNTER, TIMER, HISTOGRAM, GAUGE;
    };
    public static  void reportCounterIncr(Class name, CompactionContext  context, String metric, int value) {
        Counter counter = (Counter)getMetric(name, context,metric, MetricType.COUNTER);
        counter.inc(value);
    }

    public static  void reportCounterDecr(Class name, CompactionContext  context, String metric, int value) {
        Counter counter = (Counter)getMetric(name, context, metric, MetricType.COUNTER);
        counter.dec(value);
    }

    public static  void updateHistogram(Class name, CompactionContext  context, String metric, long value) {
        Histogram histogram = (Histogram) getMetric(name, context,metric, MetricType.HISTOGRAM);
        histogram.update(value);
    }


    public static void reportValue(Class name, CompactionContext  context, String metric, int value) {
        Meter meter = (Meter) getMetric(name, context,metric, MetricType.METER);
        meter.mark(value);
    }

    public static void resetMeterValue(Class source, CompactionContext  context, String name) {
        String metricName = createMetricName(source,context, name);
        String metricNameForStorage = metricName + METRIC_TYPE_DELIMITER + MetricType.METER;
        metricRegistry.remove(metricName);
        metricStore.remove(metricNameForStorage);
    }

    public static void setCounterValue(Class name, CompactionContext context, String metric, long value) {
        String metricName = createMetricName(name, context, metric);
        String metricNameForStorage = metricName + METRIC_TYPE_DELIMITER + MetricType.COUNTER;

        metricStore.remove(metricNameForStorage);
        metricRegistry.remove(metricName);

        Counter counter = metricRegistry.counter(metricName);
        counter.inc(value);
        metricStore.put(metricNameForStorage, counter);
    }

    public static void updateGauge(Class name, CompactionContext context, String metric, int value) {
        String metricName = createMetricName(name, context, metric);
        getMetric(name, context, metric, MetricType.GAUGE);

        AtomicInteger atomic = gaugeAtomics.get(metricName);
        if (atomic != null) {
            atomic.set(value);
        }
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
        if ( metric.equals(MetricType.HISTOGRAM)) {
            metricStore.putIfAbsent(metricNameForStorage, metricRegistry.histogram(metricName));
        }
        if ( metric.equals(MetricType.GAUGE)) {
            metricStore.computeIfAbsent(metricNameForStorage, key -> {
                AtomicInteger atomicValue = new AtomicInteger(0);
                Gauge<Integer> gauge = atomicValue::get;
                metricRegistry.register(metricName, gauge);
                gaugeAtomics.put(metricName, atomicValue);
                return gauge;
            });
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