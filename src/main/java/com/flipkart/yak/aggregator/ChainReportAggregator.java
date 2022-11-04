package com.flipkart.yak.aggregator;

import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.QueueUtils;

import java.util.*;

@Slf4j
public class ChainReportAggregator implements PolicyAggregator {

    private static final String KEY_CHAIN_ORDER = "aggregator.chain.policy.order";
    private static final String POLICY_SPLIT_CHARACTER = ",";
    private static SortedSet<Pair<String, Integer>> allPoliciesFromConfig = new TreeSet<>(new Comparator<Pair<String, Integer>>() {
        @Override
        public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
            return o1.getSecond() - o2.getSecond();
        }
    });


    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {
        configs.forEach(p -> {
            if(p.getFirst().equals(KEY_CHAIN_ORDER)) {
                String[] policyNameArray = p.getSecond().split(POLICY_SPLIT_CHARACTER);
                for (int index =0; index < policyNameArray.length; index++ ) {
                    allPoliciesFromConfig.add(new Pair<>(policyNameArray[index], index));
                }
            }
        });
    }

    @Override
    public Report applyAndCollect(Set<RegionSelectionPolicy> allPolicies, CompactionContext compactionContext) {
        Queue<Pair<String, Integer>> listOfPoliciesInOrder = new PriorityQueue<>(allPoliciesFromConfig);
        Connection connection = ConnectionInventory.getInstance().get(compactionContext.getClusterID());
        Map<String, RegionSelectionPolicy> mapOfPolicyNames = new WeakHashMap<>();
        allPolicies.forEach(e-> mapOfPolicyNames.put(e.getClass().getName(),e));
        Report finalReport = new Report(this.getClass().getName());
        Report tempReport = null;
        try {
            while (!listOfPoliciesInOrder.isEmpty()) {
                String policyName = listOfPoliciesInOrder.poll().getFirst();
                if (mapOfPolicyNames.containsKey(policyName)) {
                    if (tempReport == null) {
                        tempReport = mapOfPolicyNames.get(policyName).getReport(compactionContext, connection);
                    } else {
                        tempReport = mapOfPolicyNames.get(policyName).getReport(compactionContext, connection, tempReport);
                    }
                }
            }
        } catch (CompactionRuntimeException ce){
            log.error("could not get report from aggregation: {}", ce.getMessage());
        }
        finalReport.putAll(tempReport);
        log.info("total {} regions are being selected for major compaction in this run", finalReport.size());
        return finalReport;
    }

}
