package dev.miniq.chaos;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

final class ChaosArgs {
    private ChaosArgs() { }

    static Map<String, String> parse(String[] args, Set<String> required, Set<String> optional) {
        Map<String, String> values = new LinkedHashMap<>();
        Set<String> accepted = new java.util.HashSet<>(required);
        accepted.addAll(optional);
        for (int i = 0; i < args.length; i++) {
            String key = args[i];
            if (!accepted.contains(key)) throw new IllegalArgumentException("Unknown argument: " + key);
            if (i + 1 >= args.length) throw new IllegalArgumentException("Missing value for " + key);
            if (values.putIfAbsent(key, args[++i]) != null) {
                throw new IllegalArgumentException("Duplicate argument: " + key);
            }
        }
        for (String key : required) {
            if (!values.containsKey(key)) throw new IllegalArgumentException("Missing required argument: " + key);
        }
        return Map.copyOf(values);
    }

    static int intValue(Map<String, String> args, String name, int min, int max) {
        try {
            int value = Integer.parseInt(args.get(name));
            if (value < min || value > max) throw new IllegalArgumentException(name + " must be " + min + ".." + max);
            return value;
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("Invalid number for " + name + ": " + args.get(name), invalid);
        }
    }
}
