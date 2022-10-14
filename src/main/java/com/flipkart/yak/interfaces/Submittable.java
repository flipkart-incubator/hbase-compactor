package com.flipkart.yak.interfaces;


import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;

import java.util.Map;

public interface Submittable extends Runnable {
    public void init(CompactionContext compactionContext, Map<String, CompactionProfileConfig> profileInventory);
}
