package com.flipkart.yak.interfaces;

import com.flipkart.yak.config.ConfigListener;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import org.apache.commons.configuration.ConfigurationException;

/**
 * Produces DAL object collections i.e. Listener, Loader and Writer. Specific Implementation will implement this interface
 * like {@link  com.flipkart.yak.config.StoreFactory.ZKStoreFactory}. Calling class is expected to call init() before
 * getting any DAL implementation.
 * @param <T> Config class to initiate Factory
 */
public interface Factory<T> {
    void init(T resource) throws Exception;

    /**
     *
     * @return Store Reader DAO
     */
    AbstractConfigLoader getLoader();

    /**
     *
     * @return Store Writer DAO
     */
    AbstractConfigWriter getWriter();

    /**
     *
     * @return Watcher class to listen to Data-Change events, uses {@link  AbstractConfigLoader} internally.
     * @throws Exception
     */
    ConfigListener getConfigListener() throws Exception;

}
