package com.flipkart.yak.config;

import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import com.flipkart.yak.config.zkstore.ZKConfigLoader;
import com.flipkart.yak.config.zkstore.ZKConfigStoreWriter;

public class StoreFactory {

    private StoreFactory() {}

    private static  StoreFactory storeFactory;
    private static AbstractConfigLoader abstractConfigLoader;
    private static AbstractConfigWriter abstractConfigWriter;

    public static  StoreFactory getInstance() {
        if (storeFactory == null) {
            storeFactory = new StoreFactory();
            abstractConfigLoader = new ZKConfigLoader();
            abstractConfigWriter = new ZKConfigStoreWriter();
        }
        return storeFactory;
    }
    public  AbstractConfigLoader getLoader() {
        return abstractConfigLoader;
    }

    public AbstractConfigWriter getWriter() {
        return abstractConfigWriter;
    }
}
