package dev.miniq.chaos;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Verifies persisted broker crash recovery from publisher, consumer, and ACK logs. */
public final class StageBVerifier {
    private StageBVerifier() { }

    public static Report verify(Path before, Path after, Path acked, Path published, int expected) throws IOException {
        return verify(List.of(before, after), List.of(acked), published, expected);
    }

    /** Each received log is one process phase; each ACK log belongs to its preceding crash. */
    public static Report verify(List<Path> phases, List<Path> ackedBeforeEachRestart,
                                Path published, int expected) throws IOException {
        if (phases.isEmpty()) throw new IllegalArgumentException("at least one received-log phase is required");
        if (ackedBeforeEachRestart.size() > phases.size() - 1) {
            throw new IllegalArgumentException("there cannot be more pre-restart ACK logs than restarts");
        }
        Set<Long> ok = readIds(published);
        Map<Long,Integer> allCounts = new HashMap<>(), postRestartMinimum = new HashMap<>();
        List<Set<Long>> phaseIds = new java.util.ArrayList<>();
        int redeliveries = 0;
        for (int i=0; i<phases.size(); i++) {
            phaseIds.add(readDeliveries(phases.get(i), allCounts, i == 0 ? null : postRestartMinimum));
            redeliveries += countRedeliveryRows(phases.get(i));
        }
        Set<Long> beforeIds = phaseIds.getFirst();
        Set<Long> postRestartIds = new HashSet<>();
        for (int i=1; i<phaseIds.size(); i++) postRestartIds.addAll(phaseIds.get(i));
        Set<Long> received = new HashSet<>(); phaseIds.forEach(received::addAll);
        Set<Long> missing = new TreeSet<>(ok); missing.removeAll(received);
        Set<Long> resurrected = new TreeSet<>();
        for (int i=0; i<ackedBeforeEachRestart.size(); i++) {
            Set<Long> acknowledged = readIds(ackedBeforeEachRestart.get(i));
            for (int later=i+1; later<phaseIds.size(); later++) {
                Set<Long> overlap = new HashSet<>(acknowledged); overlap.retainAll(phaseIds.get(later));
                resurrected.addAll(overlap);
            }
        }
        Set<Long> noPostRestart = new TreeSet<>(ok); noPostRestart.removeAll(postRestartIds);
        boolean noPostRestartWereSeenBefore = beforeIds.containsAll(noPostRestart);
        Map<Integer,Set<Long>> unackedNotRedeliveredByPhase = new java.util.LinkedHashMap<>();
        for (int phase=0; phase<phaseIds.size()-1; phase++) {
            Set<Long> unackedNotRedelivered = new TreeSet<>(phaseIds.get(phase));
            if (phase < ackedBeforeEachRestart.size()) {
                unackedNotRedelivered.removeAll(readIds(ackedBeforeEachRestart.get(phase)));
            }
            Set<Long> laterReceived = new HashSet<>();
            for (int later=phase+1; later<phaseIds.size(); later++) laterReceived.addAll(phaseIds.get(later));
            unackedNotRedelivered.removeAll(laterReceived);
            if (!unackedNotRedelivered.isEmpty()) {
                unackedNotRedeliveredByPhase.put(phase+1, unackedNotRedelivered);
            }
        }
        boolean unackedNotRedeliveredWithinBound = unackedNotRedeliveredByPhase.values()
                .stream().allMatch(ids -> ids.size() <= 1);
        int duplicates = allCounts.values().stream().mapToInt(n -> Math.max(0, n - 1)).sum();
        Set<Long> crossPhaseIds = new HashSet<>();
        for (int i=0; i<phaseIds.size(); i++) for (int j=i+1; j<phaseIds.size(); j++) {
            Set<Long> overlap = new HashSet<>(phaseIds.get(i)); overlap.retainAll(phaseIds.get(j));
            crossPhaseIds.addAll(overlap);
        }
        boolean minimumsStartAtOne = postRestartMinimum.values().stream().allMatch(count -> count == 1);
        boolean pass = missing.isEmpty() && resurrected.isEmpty() && noPostRestartWereSeenBefore
                && unackedNotRedeliveredWithinBound && minimumsStartAtOne;
        return new Report(ok.size(), missing, resurrected, noPostRestart, beforeIds,
                noPostRestartWereSeenBefore, unackedNotRedeliveredByPhase, unackedNotRedeliveredWithinBound,
                duplicates, redeliveries, crossPhaseIds.size(),
                postRestartMinimum, pass);
    }

    private static Set<Long> readIds(Path file) throws IOException {
        Set<Long> ids = new HashSet<>();
        for (String line : completeLines(file)) try { ids.add(Long.parseLong(line.trim())); } catch (NumberFormatException ignored) { }
        return ids;
    }

    private static Set<Long> readDeliveries(Path file, Map<Long,Integer> all, Map<Long,Integer> postRestart) throws IOException {
        Set<Long> ids = new HashSet<>();
        for (String line : completeLines(file)) {
            String[] cols = line.trim().split("\\s+");
            if (cols.length != 2) continue;
            try {
                long id = Long.parseLong(cols[0]); int count = Integer.parseInt(cols[1]);
                if (id < 1 || count < 1) continue;
                all.merge(id, 1, Integer::sum); ids.add(id);
                if (postRestart != null) postRestart.merge(id, count, Math::min);
            } catch (NumberFormatException ignored) { }
        }
        return ids;
    }

    private static int countRedeliveryRows(Path file) throws IOException {
        int count = 0;
        for (String line : completeLines(file)) {
            String[] cols = line.trim().split("\\s+");
            if (cols.length == 2) try { if (Integer.parseInt(cols[1]) > 1) count++; } catch (NumberFormatException ignored) { }
        }
        return count;
    }

    private static List<String> completeLines(Path file) throws IOException {
        if (!Files.exists(file)) return List.of();
        byte[] bytes = Files.readAllBytes(file);
        int complete = bytes.length;
        while (complete > 0 && bytes[complete - 1] != '\n') complete--;
        if (complete == 0) return List.of();
        return List.of(new String(bytes, 0, complete, java.nio.charset.StandardCharsets.UTF_8).split("\\R"));
    }

    public record Report(int publishedOk, Set<Long> missing, Set<Long> resurrected, Set<Long> noPostRestart,
                         Set<Long> receivedBeforeRestart, boolean noPostRestartWereSeenBefore,
                         Map<Integer,Set<Long>> unackedNotRedeliveredByPhase,
                         boolean unackedNotRedeliveredWithinBound,
                         int duplicates, int redeliveries, int crossRestartDuplicates,
                         Map<Long,Integer> postRestartMinimum, boolean passed) {
        public int minPostRestartDeliveryCount() {
            return postRestartMinimum.values().stream().mapToInt(Integer::intValue).min().orElse(0);
        }

        public String format() {
            Set<Long> badRestartCounts = new TreeSet<>();
            postRestartMinimum.forEach((id,count) -> { if (count != 1) badRestartCounts.add(id); });
            return "Stage B verification: " + (passed ? "PASS" : "FAIL") + "\nPublished OK ids: " + publishedOk
                    + "\nMissing ids: " + (missing.isEmpty() ? "none" : missing)
                    + "\nResurrected ids: " + (resurrected.isEmpty() ? "none" : resurrected)
                    + "\nIds with no post-restart delivery: " + noPostRestart.size()
                    + "\nNo-post-restart ids were received before restart: " + noPostRestartWereSeenBefore
                    + "\nUnacked and not redelivered ids by consumer phase: " + unackedNotRedeliveredByPhase
                    + "\nAt most one unacked/no-redelivery id per consumer phase: "
                    + unackedNotRedeliveredWithinBound
                    + "\nDuplicate count: " + duplicates
                    + "\nRedelivery count (deliveryCount > 1 rows): " + redeliveries
                    + "\nCross-restart duplicate ids: " + crossRestartDuplicates
                    + "\nMinimum post-restart deliveryCount: "
                    + (postRestartMinimum.isEmpty() ? "none" : minPostRestartDeliveryCount())
                    + "\nPost-restart ids checked: " + postRestartMinimum.size()
                    + "\nPost-restart ids whose minimum deliveryCount was not 1: "
                    + (badRestartCounts.isEmpty() ? "none" : badRestartCounts);
        }
    }
}
