package net.czqu.rmtt.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.slf4j.helpers.MarkerIgnoringBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * Comprehensive verification of the {@code net.czqu.rmtt.logging} facade. A plain main (no JUnit
 * in this tree) that exercises every level against every backend:
 *
 * <ul>
 *   <li>{@link MessageFormatter}: 0/1/2/N args, {@code \{} } escape, missing args, null handling.</li>
 *   <li>Factory auto-discovery: with slf4j on the classpath the default factory must be SLF4J.</li>
 *   <li>Level matrix (5 configured levels x 5 emitted levels) x 3 backends (JUL / SLF4J / Log4J2):
 *       asserts both the {@code isXxxEnabled()} matrix and the actually captured lines. The hard
 *       requirement is that configuring {@code info} never produces {@code debug}/{@code trace}.</li>
 * </ul>
 *
 * <p>Run with: {@code mvn -o test-compile} then assemble the test classpath from {@code ~/.m2}.</p>
 */
public final class LoggingVerifier {

    private static final int TRACE = 0;
    private static final int DEBUG = 1;
    private static final int INFO = 2;
    private static final int WARN = 3;
    private static final int ERROR = 4;
    private static final String[] LEVEL_NAMES = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR"};

    private static int failures = 0;
    private static int checks = 0;

    private LoggingVerifier() {
    }

    public static void main(String[] args) throws Exception {
        testMessageFormatter();
        testFactoryDiscovery();
        testJdkBackend();
        testSlf4jBackend();
        testSlf4jRealBinding();
        testLog4j2Backend();

        if (failures == 0) {
            System.out.println("LOGGING VERIFIER PASSED (" + checks + " checks)");
        } else {
            System.err.println("LOGGING VERIFIER FAILED: " + failures + " / " + checks + " checks failed");
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------- formatter

    private static void testMessageFormatter() {
        eq("format null pattern", "null", MessageFormatter.format(null));
        eq("format no args", "a {} b", MessageFormatter.format("a {} b"));
        eq("format 1 arg", "hello world", MessageFormatter.format("hello {}", "world"));
        eq("format 2 args", "a=1 b=2", MessageFormatter.format("a={} b={}", 1, 2));
        eq("format N args", "x y z", MessageFormatter.format("{} {} {}", "x", "y", "z"));
        eq("format escape", "a {x} b=1", MessageFormatter.format("a \\{x} b={}", 1));
        eq("format missing arg", "v={}", MessageFormatter.format("v={}"));
        eq("format null arg", "v=null", MessageFormatter.format("v={}", new Object[]{null}));
        eq("format extra args ignored", "a b", MessageFormatter.format("{} {}", "a", "b", "c"));
    }

    // ---------------------------------------------------------------- discovery

    private static void testFactoryDiscovery() {
        InternalLoggerFactory discovered = InternalLoggerFactory.getDefaultFactory();
        check("discovery selects SLF4J (slf4j-api on classpath)",
                discovered instanceof Slf4JLoggerFactory);
        InternalLogger byClass = InternalLoggerFactory.getLogger(String.class);
        eq("getLogger(Class) name", "java.lang.String", byClass.name());
        InternalLogger byName = InternalLoggerFactory.getLogger("com.example.Marker");
        eq("getLogger(String) name", "com.example.Marker", byName.name());

        InternalLoggerFactory saved = InternalLoggerFactory.getDefaultFactory();
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        InternalLogger forced = InternalLoggerFactory.getLogger("forced.jdk");
        check("setDefaultFactory override -> JdkLogger", forced instanceof JdkLogger);
        InternalLoggerFactory.setDefaultFactory(saved);
        InternalLogger restored = InternalLoggerFactory.getLogger("forced.jdk");
        check("setDefaultFactory restore -> SLF4J again", restored instanceof Slf4JLogger);
    }

    // ---------------------------------------------------------------- JUL backend

    private static void testJdkBackend() throws Exception {
        for (int configLevel = TRACE; configLevel <= ERROR; configLevel++) {
            String name = "matrix.jul.c" + configLevel;
            java.util.logging.Logger jul = java.util.logging.Logger.getLogger(name);
            jul.setUseParentHandlers(false);
            jul.setLevel(toJdkLevel(configLevel));
            CapturingJulHandler handler = new CapturingJulHandler();
            handler.setLevel(java.util.logging.Level.ALL);
            jul.addHandler(handler);
            try {
                InternalLogger logger = JdkLoggerFactory.INSTANCE.newInstance(name);
                runLevelMatrix("JUL@c" + LEVEL_NAMES[configLevel], logger, handler, configLevel);
            } finally {
                jul.removeHandler(handler);
                handler.close();
            }
        }
    }

    private static java.util.logging.Level toJdkLevel(int idx) {
        switch (idx) {
            case TRACE: return java.util.logging.Level.FINEST;
            case DEBUG: return java.util.logging.Level.FINE;
            case INFO: return java.util.logging.Level.INFO;
            case WARN: return java.util.logging.Level.WARNING;
            default: return java.util.logging.Level.SEVERE;
        }
    }

    private static final class CapturingJulHandler extends Handler implements Captured {
        final List<String> messages = new CopyOnWriteArrayList<>();

        @Override
        public void publish(LogRecord record) {
            messages.add(LEVEL_NAMES[fromJdkLevel(record.getLevel())] + ":" + record.getMessage());
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            messages.clear();
        }

        @Override
        public List<String> messages() {
            return messages;
        }
    }

    private static int fromJdkLevel(java.util.logging.Level lvl) {
        if (lvl == java.util.logging.Level.FINEST || lvl == java.util.logging.Level.FINER) {
            return TRACE;
        }
        if (lvl == java.util.logging.Level.FINE) {
            return DEBUG;
        }
        if (lvl == java.util.logging.Level.INFO) {
            return INFO;
        }
        if (lvl == java.util.logging.Level.WARNING) {
            return WARN;
        }
        return ERROR;
    }

    // ---------------------------------------------------------------- SLF4J backend (adapter)

    private static void testSlf4jBackend() {
        for (int configLevel = TRACE; configLevel <= ERROR; configLevel++) {
            ConfigurableSlf4jLogger adapter =
                    new ConfigurableSlf4jLogger("matrix.slf4j.c" + configLevel, configLevel);
            InternalLogger logger = new Slf4JLogger(adapter);
            runLevelMatrix("SLF4J@c" + LEVEL_NAMES[configLevel], logger, adapter, configLevel);
        }
    }

    /** Minimal org.slf4j.Logger that behaves like a level-configured binding. */
    private static final class ConfigurableSlf4jLogger extends MarkerIgnoringBase implements Captured {
        private final String name;
        private final int threshold;
        final List<String> messages = new CopyOnWriteArrayList<>();

        ConfigurableSlf4jLogger(String name, int threshold) {
            this.name = name;
            this.threshold = threshold;
        }

        @Override
        public String getName() {
            return name;
        }

        private void log(int level, String msg) {
            if (level >= threshold) {
                messages.add(LEVEL_NAMES[level] + ":" + msg);
            }
        }

        private boolean enabled(int level) {
            return level >= threshold;
        }

        @Override public boolean isTraceEnabled() { return enabled(TRACE); }
        @Override public void trace(String msg) { log(TRACE, msg); }
        @Override public void trace(String format, Object arg) { log(TRACE, format); }
        @Override public void trace(String format, Object arg1, Object arg2) { log(TRACE, format); }
        @Override public void trace(String format, Object... arguments) { log(TRACE, format); }
        @Override public void trace(String msg, Throwable t) { log(TRACE, msg); }

        @Override public boolean isDebugEnabled() { return enabled(DEBUG); }
        @Override public void debug(String msg) { log(DEBUG, msg); }
        @Override public void debug(String format, Object arg) { log(DEBUG, format); }
        @Override public void debug(String format, Object arg1, Object arg2) { log(DEBUG, format); }
        @Override public void debug(String format, Object... arguments) { log(DEBUG, format); }
        @Override public void debug(String msg, Throwable t) { log(DEBUG, msg); }

        @Override public boolean isInfoEnabled() { return enabled(INFO); }
        @Override public void info(String msg) { log(INFO, msg); }
        @Override public void info(String format, Object arg) { log(INFO, format); }
        @Override public void info(String format, Object arg1, Object arg2) { log(INFO, format); }
        @Override public void info(String format, Object... arguments) { log(INFO, format); }
        @Override public void info(String msg, Throwable t) { log(INFO, msg); }

        @Override public boolean isWarnEnabled() { return enabled(WARN); }
        @Override public void warn(String msg) { log(WARN, msg); }
        @Override public void warn(String format, Object arg) { log(WARN, format); }
        @Override public void warn(String format, Object arg1, Object arg2) { log(WARN, format); }
        @Override public void warn(String format, Object... arguments) { log(WARN, format); }
        @Override public void warn(String msg, Throwable t) { log(WARN, msg); }

        @Override public boolean isErrorEnabled() { return enabled(ERROR); }
        @Override public void error(String msg) { log(ERROR, msg); }
        @Override public void error(String format, Object arg) { log(ERROR, format); }
        @Override public void error(String format, Object arg1, Object arg2) { log(ERROR, format); }
        @Override public void error(String format, Object... arguments) { log(ERROR, format); }
        @Override public void error(String msg, Throwable t) { log(ERROR, msg); }

        @Override
        public List<String> messages() {
            return messages;
        }
    }

    // ---------------------------------------------------------------- SLF4J real binding (slf4j-simple)

    private static void testSlf4jRealBinding() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(buf, true, StandardCharsets.UTF_8.name()));
        try {
            InternalLogger logger = InternalLoggerFactory.getLogger("matrix.real");
            logger.debug("hidden-debug-msg");
            logger.info("visible-info-msg");
        } finally {
            System.setErr(original);
        }
        String out = buf.toString(StandardCharsets.UTF_8.name());
        check("real binding: info captured", out.contains("visible-info-msg"));
        check("real binding: info level shows INFO", out.contains("INFO"));
        check("real binding: debug suppressed at info threshold", !out.contains("hidden-debug-msg"));
    }

    // ---------------------------------------------------------------- Log4J2 backend

    private static void testLog4j2Backend() {
        for (int configLevel = TRACE; configLevel <= ERROR; configLevel++) {
            String ctxName = "matrix.log4j2.c" + configLevel;
            org.apache.logging.log4j.core.LoggerContext ctx =
                    new org.apache.logging.log4j.core.LoggerContext(ctxName);
            ctx.start();
            org.apache.logging.log4j.core.config.Configuration cfg = ctx.getConfiguration();
            CapturingLog4jAppender appender = new CapturingLog4jAppender(ctxName + "-app");
            appender.start();
            cfg.addAppender(appender);
            Level lvl = toLog4jLevel(configLevel);
            cfg.getRootLogger().setLevel(lvl);
            cfg.getRootLogger().addAppender(appender, Level.ALL, null);
            ctx.updateLoggers();
            InternalLogger logger = new Log4J2Logger(ctx.getLogger("matrix"));
            runLevelMatrix("LOG4J2@c" + LEVEL_NAMES[configLevel], logger, appender, configLevel);
            ctx.stop();
        }
    }

    private static Level toLog4jLevel(int idx) {
        switch (idx) {
            case TRACE: return Level.TRACE;
            case DEBUG: return Level.DEBUG;
            case INFO: return Level.INFO;
            case WARN: return Level.WARN;
            default: return Level.ERROR;
        }
    }

    private static final class CapturingLog4jAppender extends AbstractAppender implements Captured {
        final List<String> messages = new CopyOnWriteArrayList<>();

        CapturingLog4jAppender(String name) {
            super(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            int idx;
            switch (event.getLevel().getStandardLevel()) {
                case TRACE: idx = TRACE; break;
                case DEBUG: idx = DEBUG; break;
                case INFO: idx = INFO; break;
                case WARN: idx = WARN; break;
                default: idx = ERROR; break;
            }
            messages.add(LEVEL_NAMES[idx] + ":" + event.getMessage().getFormattedMessage());
        }

        @Override
        public List<String> messages() {
            return messages;
        }
    }

    // ---------------------------------------------------------------- shared matrix driver

    private interface Captured {
        List<String> messages();
    }

    private static void runLevelMatrix(String label, InternalLogger logger, Captured captured, int configLevel) {
        logger.trace("trace-msg");
        logger.debug("debug-msg");
        logger.info("info-msg");
        logger.warn("warn-msg");
        logger.error("error-msg");

        for (int lvl = TRACE; lvl <= ERROR; lvl++) {
            boolean expectedEnabled = lvl >= configLevel;
            boolean actualEnabled = isEnabled(logger, lvl);
            check(label + " isEnabled(" + LEVEL_NAMES[lvl] + ")=" + expectedEnabled,
                    actualEnabled == expectedEnabled);
        }

        List<String> lines = captured.messages();
        boolean[] emitted = new boolean[5];
        for (int lvl = TRACE; lvl <= ERROR; lvl++) {
            emitted[lvl] = containsLevel(lines, LEVEL_NAMES[lvl]);
            boolean expected = lvl >= configLevel;
            check(label + " captured(" + LEVEL_NAMES[lvl] + ")=" + expected, emitted[lvl] == expected);
        }

        List<String> capturedPrefixes = new ArrayList<>();
        for (int lvl = TRACE; lvl <= ERROR; lvl++) {
            if (emitted[lvl]) {
                capturedPrefixes.add(LEVEL_NAMES[lvl]);
            }
        }
        List<String> expectedPrefixes = new ArrayList<>();
        for (int lvl = configLevel; lvl <= ERROR; lvl++) {
            expectedPrefixes.add(LEVEL_NAMES[lvl]);
        }
        check(label + " captured level order = " + expectedPrefixes, capturedPrefixes.equals(expectedPrefixes));
    }

    private static boolean containsLevel(List<String> lines, String tag) {
        for (String line : lines) {
            if (line.startsWith(tag + ":")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isEnabled(InternalLogger logger, int lvl) {
        switch (lvl) {
            case TRACE: return logger.isTraceEnabled();
            case DEBUG: return logger.isDebugEnabled();
            case INFO: return logger.isInfoEnabled();
            case WARN: return logger.isWarnEnabled();
            default: return logger.isErrorEnabled();
        }
    }

    // ---------------------------------------------------------------- plumbing

    private static void eq(String what, String expected, String actual) {
        checks++;
        if (!expected.equals(actual)) {
            failures++;
            System.err.println("FAIL " + what + ": expected [" + expected + "] got [" + actual + "]");
        }
    }

    private static void check(String what, boolean ok) {
        checks++;
        if (!ok) {
            failures++;
            System.err.println("FAIL " + what);
        }
    }
}
