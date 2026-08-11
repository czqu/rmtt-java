package net.czqu.rmtt.logging;

import org.apache.logging.log4j.LogManager;

/** {@link InternalLoggerFactory} producing Log4j 2 backed loggers. */
public class Log4J2LoggerFactory extends InternalLoggerFactory {

    /** Create a factory producing Log4j 2 backed loggers. */
    public Log4J2LoggerFactory() {
    }

    @Override
    public InternalLogger newInstance(String name) {
        return new Log4J2Logger(LogManager.getLogger(name));
    }
}
