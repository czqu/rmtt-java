package net.czqu.rmtt.logging;

/**
 * Factory for {@link InternalLogger} instances, mirroring {@code io.netty.util.internal.logging}.
 *
 * <p>Library code obtains loggers via {@link #getLogger(Class)} / {@link #getLogger(String)}. The
 * concrete backend is selected lazily by {@link #getDefaultFactory()}: on first use it auto-discovers
 * SLF4J, then Log4J2, then falls back to {@link java.util.logging} (JUL). Applications that bring
 * their own logging library can override the backend with {@link #setDefaultFactory(InternalLoggerFactory)}.</p>
 */
public abstract class InternalLoggerFactory {

    /** No-op; subclasses carry the concrete backend selection. */
    protected InternalLoggerFactory() {
    }

    private static volatile InternalLoggerFactory defaultFactory;

    /**
     * The currently active factory, auto-discovering a backend on first use.
     *
     * @return the currently active factory
     */
    public static InternalLoggerFactory getDefaultFactory() {
        if (defaultFactory == null) {
            defaultFactory = DefaultFactoryHolder.factory();
        }
        return defaultFactory;
    }

    /**
     * Replace the default factory. Safe to call at any time; subsequent {@link #getLogger} calls use
     * the new factory.
     *
     * @param factory the factory to use, must not be null
     * @throws NullPointerException if {@code factory} is null
     */
    public static void setDefaultFactory(InternalLoggerFactory factory) {
        if (factory == null) {
            throw new NullPointerException("factory");
        }
        defaultFactory = factory;
    }

    /**
     * A logger named after the given class.
     *
     * @param clazz the class that will own the logger
     * @return a logger named after {@code clazz}
     */
    public static InternalLogger getLogger(Class<?> clazz) {
        return getLogger(clazz.getName());
    }

    /**
     * A logger for the given name from the current default factory.
     *
     * @param name the logger name
     * @return a logger for the given name
     */
    public static InternalLogger getLogger(String name) {
        return getDefaultFactory().newInstance(name);
    }

    /**
     * Create a logger instance for the given name.
     *
     * @param name the logger name
     * @return a new logger instance for the given name
     */
    public abstract InternalLogger newInstance(String name);

    private static final class DefaultFactoryHolder {

        private static InternalLoggerFactory factory() {
            try {
                Class.forName("org.slf4j.Logger", true, InternalLoggerFactory.class.getClassLoader());
                return new Slf4JLoggerFactory();
            } catch (ClassNotFoundException | NoClassDefFoundError ignored) {
                // fall through
            }
            try {
                Class.forName("org.apache.logging.log4j.Logger", true,
                        InternalLoggerFactory.class.getClassLoader());
                return new Log4J2LoggerFactory();
            } catch (ClassNotFoundException | NoClassDefFoundError ignored) {
                // fall through
            }
            return new JdkLoggerFactory();
        }
    }
}
