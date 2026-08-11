package net.czqu.rmtt.logging;

/**
 * slf4j-style placeholder formatter used by the {@code format, arg...} logger overloads. A
 * placeholder is a pair of literal braces ({@code "{}"} denotes the sequence); each placeholder is
 * replaced by the next argument.
 *
 * <p>Backslash-escaped placeholders are rendered as a literal left brace. Missing arguments keep
 * their placeholder text in the output.</p>
 */
public final class MessageFormatter {

    private MessageFormatter() {
    }

    /**
     * Fill {@code {}} placeholders in {@code pattern} with the supplied arguments.
     *
     * @param pattern the message pattern; null is formatted as the literal string "null"
     * @param args    the replacement arguments; extra arguments are ignored
     * @return the formatted string
     */
    public static String format(String pattern, Object... args) {
        if (pattern == null) {
            return "null";
        }
        if (args == null || args.length == 0) {
            return pattern;
        }
        StringBuilder sb = new StringBuilder(pattern.length() + 32);
        int argIdx = 0;
        int i = 0;
        int n = pattern.length();
        while (i < n) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < n && pattern.charAt(i + 1) == '{') {
                sb.append('{');
                i += 2;
                continue;
            }
            if (c == '{' && i + 1 < n && pattern.charAt(i + 1) == '}') {
                sb.append(argIdx < args.length ? String.valueOf(args[argIdx++]) : "{}");
                i += 2;
                continue;
            }
            sb.append(c);
            i++;
        }
        return sb.toString();
    }
}
