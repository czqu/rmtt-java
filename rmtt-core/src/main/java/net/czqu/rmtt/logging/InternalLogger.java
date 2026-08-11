package net.czqu.rmtt.logging;

/**
 * A minimal, Netty-style internal logging abstraction.
 *
 * <p>Mirrors {@code io.netty.util.internal.logging}: library code only talks to this interface, so
 * the concrete backend is chosen by the user (or auto-discovered) without coupling the library to
 * any specific logging framework. Every method family (trace/debug/info/warn/error) pairs an
 * {@code isXxxEnabled()} guard with formatting-capable log calls plus a {@code Throwable} variant.
 */
public interface InternalLogger {

    /**
     * The logger name.
     *
     * @return the logger name
     */
    String name();

    /**
     * Whether trace is enabled for this logger.
     *
     * @return true if trace is enabled for this logger
     */
    boolean isTraceEnabled();

    /**
     * Log a plain trace message.
     *
     * @param msg the message text
     */
    void trace(String msg);

    /**
     * Log a formatted trace message with one argument.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param arg    the first argument
     */
    void trace(String format, Object arg);

    /**
     * Log a formatted trace message with two arguments.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param argA   the first argument
     * @param argB   the second argument
     */
    void trace(String format, Object argA, Object argB);

    /**
     * Log a formatted trace message with a variable number of arguments.
     *
     * @param format    the message pattern with {@code {}} placeholders
     * @param arguments the arguments
     */
    void trace(String format, Object... arguments);

    /**
     * Log a trace message with a stack trace.
     *
     * @param msg the message text
     * @param t   the throwable
     */
    void trace(String msg, Throwable t);

    /**
     * Whether debug is enabled for this logger.
     *
     * @return true if debug is enabled for this logger
     */
    boolean isDebugEnabled();

    /**
     * Log a plain debug message.
     *
     * @param msg the message text
     */
    void debug(String msg);

    /**
     * Log a formatted debug message with one argument.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param arg    the first argument
     */
    void debug(String format, Object arg);

    /**
     * Log a formatted debug message with two arguments.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param argA   the first argument
     * @param argB   the second argument
     */
    void debug(String format, Object argA, Object argB);

    /**
     * Log a formatted debug message with a variable number of arguments.
     *
     * @param format    the message pattern with {@code {}} placeholders
     * @param arguments the arguments
     */
    void debug(String format, Object... arguments);

    /**
     * Log a debug message with a stack trace.
     *
     * @param msg the message text
     * @param t   the throwable
     */
    void debug(String msg, Throwable t);

    /**
     * Whether info is enabled for this logger.
     *
     * @return true if info is enabled for this logger
     */
    boolean isInfoEnabled();

    /**
     * Log a plain info message.
     *
     * @param msg the message text
     */
    void info(String msg);

    /**
     * Log a formatted info message with one argument.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param arg    the first argument
     */
    void info(String format, Object arg);

    /**
     * Log a formatted info message with two arguments.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param argA   the first argument
     * @param argB   the second argument
     */
    void info(String format, Object argA, Object argB);

    /**
     * Log a formatted info message with a variable number of arguments.
     *
     * @param format    the message pattern with {@code {}} placeholders
     * @param arguments the arguments
     */
    void info(String format, Object... arguments);

    /**
     * Log an info message with a stack trace.
     *
     * @param msg the message text
     * @param t   the throwable
     */
    void info(String msg, Throwable t);

    /**
     * Whether warn is enabled for this logger.
     *
     * @return true if warn is enabled for this logger
     */
    boolean isWarnEnabled();

    /**
     * Log a plain warn message.
     *
     * @param msg the message text
     */
    void warn(String msg);

    /**
     * Log a formatted warn message with one argument.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param arg    the first argument
     */
    void warn(String format, Object arg);

    /**
     * Log a formatted warn message with two arguments.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param argA   the first argument
     * @param argB   the second argument
     */
    void warn(String format, Object argA, Object argB);

    /**
     * Log a formatted warn message with a variable number of arguments.
     *
     * @param format    the message pattern with {@code {}} placeholders
     * @param arguments the arguments
     */
    void warn(String format, Object... arguments);

    /**
     * Log a warn message with a stack trace.
     *
     * @param msg the message text
     * @param t   the throwable
     */
    void warn(String msg, Throwable t);

    /**
     * Whether error is enabled for this logger.
     *
     * @return true if error is enabled for this logger
     */
    boolean isErrorEnabled();

    /**
     * Log a plain error message.
     *
     * @param msg the message text
     */
    void error(String msg);

    /**
     * Log a formatted error message with one argument.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param arg    the first argument
     */
    void error(String format, Object arg);

    /**
     * Log a formatted error message with two arguments.
     *
     * @param format the message pattern with {@code {}} placeholders
     * @param argA   the first argument
     * @param argB   the second argument
     */
    void error(String format, Object argA, Object argB);

    /**
     * Log a formatted error message with a variable number of arguments.
     *
     * @param format    the message pattern with {@code {}} placeholders
     * @param arguments the arguments
     */
    void error(String format, Object... arguments);

    /**
     * Log an error message with a stack trace.
     *
     * @param msg the message text
     * @param t   the throwable
     */
    void error(String msg, Throwable t);
}
