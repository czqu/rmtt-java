package net.czqu.rmtt.logging;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link InternalLogger} backed by {@link java.util.logging} (JUL), the JDK built-in logger.
 *
 * <p>Level mapping: trace=FINEST, debug=FINE, info=INFO, warn=WARNING, error=SEVERE. Every log call
 * is gated by {@code logger.isLoggable(...)}, so a lower configured level never leaks through.</p>
 */
final class JdkLogger implements InternalLogger {

    private final transient Logger logger;
    private final String name;

    JdkLogger(Logger logger, String name) {
        this.logger = logger;
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isLoggable(Level.FINEST);
    }

    @Override
    public void trace(String msg) {
        logger.log(Level.FINEST, msg);
    }

    @Override
    public void trace(String format, Object arg) {
        logger.log(Level.FINEST, MessageFormatter.format(format, arg));
    }

    @Override
    public void trace(String format, Object argA, Object argB) {
        logger.log(Level.FINEST, MessageFormatter.format(format, argA, argB));
    }

    @Override
    public void trace(String format, Object... arguments) {
        logger.log(Level.FINEST, MessageFormatter.format(format, arguments));
    }

    @Override
    public void trace(String msg, Throwable t) {
        logger.log(Level.FINEST, msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    @Override
    public void debug(String msg) {
        logger.log(Level.FINE, msg);
    }

    @Override
    public void debug(String format, Object arg) {
        logger.log(Level.FINE, MessageFormatter.format(format, arg));
    }

    @Override
    public void debug(String format, Object argA, Object argB) {
        logger.log(Level.FINE, MessageFormatter.format(format, argA, argB));
    }

    @Override
    public void debug(String format, Object... arguments) {
        logger.log(Level.FINE, MessageFormatter.format(format, arguments));
    }

    @Override
    public void debug(String msg, Throwable t) {
        logger.log(Level.FINE, msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    @Override
    public void info(String msg) {
        logger.log(Level.INFO, msg);
    }

    @Override
    public void info(String format, Object arg) {
        logger.log(Level.INFO, MessageFormatter.format(format, arg));
    }

    @Override
    public void info(String format, Object argA, Object argB) {
        logger.log(Level.INFO, MessageFormatter.format(format, argA, argB));
    }

    @Override
    public void info(String format, Object... arguments) {
        logger.log(Level.INFO, MessageFormatter.format(format, arguments));
    }

    @Override
    public void info(String msg, Throwable t) {
        logger.log(Level.INFO, msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isLoggable(Level.WARNING);
    }

    @Override
    public void warn(String msg) {
        logger.log(Level.WARNING, msg);
    }

    @Override
    public void warn(String format, Object arg) {
        logger.log(Level.WARNING, MessageFormatter.format(format, arg));
    }

    @Override
    public void warn(String format, Object argA, Object argB) {
        logger.log(Level.WARNING, MessageFormatter.format(format, argA, argB));
    }

    @Override
    public void warn(String format, Object... arguments) {
        logger.log(Level.WARNING, MessageFormatter.format(format, arguments));
    }

    @Override
    public void warn(String msg, Throwable t) {
        logger.log(Level.WARNING, msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    @Override
    public void error(String msg) {
        logger.log(Level.SEVERE, msg);
    }

    @Override
    public void error(String format, Object arg) {
        logger.log(Level.SEVERE, MessageFormatter.format(format, arg));
    }

    @Override
    public void error(String format, Object argA, Object argB) {
        logger.log(Level.SEVERE, MessageFormatter.format(format, argA, argB));
    }

    @Override
    public void error(String format, Object... arguments) {
        logger.log(Level.SEVERE, MessageFormatter.format(format, arguments));
    }

    @Override
    public void error(String msg, Throwable t) {
        logger.log(Level.SEVERE, msg, t);
    }
}
