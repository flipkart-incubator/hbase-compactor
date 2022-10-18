package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.Report;

import java.util.List;

public interface PolicyAggregator extends Configurable {
    Report aggregateReport(List<Report> reports);
}
