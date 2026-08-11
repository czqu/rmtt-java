package net.czqu.rmtt.logging;

import java.util.logging.Logger;

/** {@link InternalLoggerFactory} producing {@link JdkLogger} (JUL) instances. */
public class JdkLoggerFactory extends InternalLoggerFactory {

    /** Shared singleton usable as the default factory. */
    public static final JdkLoggerFactory INSTANCE = new JdkLoggerFactory();

    /** Create a factory producing JUL-backed loggers. */
    public JdkLoggerFactory() {
    }

    @Override
    public InternalLogger newInstance(String name) {
        return new JdkLogger(Logger.getLogger(name), name);
    }
}
