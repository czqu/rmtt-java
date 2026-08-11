package net.czqu.rmtt.logging;

import org.slf4j.LoggerFactory;

/** {@link InternalLoggerFactory} producing SLF4J-backed loggers. */
public class Slf4JLoggerFactory extends InternalLoggerFactory {

    /** Create the factory; safe to install as a singleton. */
    public Slf4JLoggerFactory() {
    }

    @Override
    public InternalLogger newInstance(String name) {
        return new Slf4JLogger(LoggerFactory.getLogger(name));
    }
}
