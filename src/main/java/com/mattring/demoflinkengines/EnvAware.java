package com.mattring.demoflinkengines;

/**
 *
 * @author Matthew
 * @param <T>
 */
public class EnvAware<T> {

    protected final T env;

    public EnvAware(T env) {
        this.env = env;
    }

}
