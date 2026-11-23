package dev.miniq.chaos;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class ChaosStageBProcessTest {
    @TempDir Path dir;
    private final ChaosProcesses ps = new ChaosProcesses();
    private static final Duration LIMIT = Duration.ofSeconds(60);
    private static final Pattern READY_PORT = Pattern.compile("MINIQ_READY port=(\\d+)");

    @AfterEach void cleanup() { ps.close(); }

    @Test void consumerInFlightSurvivesBrokerCrash() throws Exception {
        runB1("default", "default", 1000, 300, "5");
    }

    @Test void consumerInFlightSurvivesBrokerCrashWithEveryWriteFsync() throws Exception {
        runB1("EVERY_WRITE", "default", 1000, 300, "5");
    }

    @Test void consumerInFlightSurvivesBrokerCrashWithMaxInFlightOne() throws Exception {
        runB1("default", "1", 1000, 300, "5");
    }

    @Test void consumerInFlightSurvivesBrokerCrashWithMaxInFlightOneThousand() throws Exception {
        runB1("default", "1000", 500, 1, "200");
    }

    private void runB1(String fsync, String maxInFlight, int messageCount, int crashAfterReceives,
                       String processingDelayMs) throws Exception {
        Path data=dir.resolve("b1-data"), before=dir.resolve("b1-a.log"), after=dir.resolve("b1-b.log");
        Path ack=dir.resolve("b1-acked.log"), ok=dir.resolve("b1-ok.log");
        Server first=startServer(data,fsync,maxInFlight); publish(first.port,messageCount,ok,null);
        var a=consumer(first.port,before,ack,processingDelayMs); awaitDistinct(before,crashAfterReceives,messageCount,a);
        ps.destroyForcibly(first.child); assertNoLocks(data); assertConsumerExits(a);
        Server restarted=startServer(data,fsync,maxInFlight); assertRecovery(restarted,messageCount);
        consumer(restarted.port,after,null,null);
        var report=await(List.of(before,after),List.of(ack),ok,messageCount);
        assertVerified(report); assertThat(number(restarted.recovered,"acked")).isGreaterThanOrEqualTo(completeLines(ack));
    }

    @Test void brokerCrashDuringPublishingPreservesEveryPublishConfirmedOk() throws Exception {
        Path data=dir.resolve("b2-data"), before=dir.resolve("b2-a.log"), after=dir.resolve("b2-b.log");
        Path ack=dir.resolve("b2-acked.log"), ok=dir.resolve("b2-ok.log");
        Server first=startServer(data); var pub=publish(first.port,1000,ok,"2");
        var a=consumer(first.port,before,ack,"5"); awaitLineCount(ok,400);
        assertThat(pub.isAlive()).as("publisher still publishing at broker kill").isTrue();
        ps.destroyForcibly(first.child); assertNoLocks(data); assertConsumerExits(a);
        assertThat(pub.awaitExit(LIMIT)).as("publisher interrupted by broker kill").isNotZero();
        int okCount=completeLines(ok); assertThat(okCount).isGreaterThanOrEqualTo(400);
        Server restarted=startServer(data); assertRecovery(restarted,okCount);
        consumer(restarted.port,after,null,null);
        var report=await(List.of(before,after),List.of(ack),ok,okCount);
        assertVerified(report); assertThat(number(restarted.recovered,"messages")).isGreaterThanOrEqualTo(okCount);
    }

    @Test void twoBrokerCrashesPreserveConfirmedPublishesAcrossAllPhases() throws Exception {
        Path data=dir.resolve("b3-data"), aLog=dir.resolve("b3-a.log"), bLog=dir.resolve("b3-b.log"), cLog=dir.resolve("b3-c.log");
        Path ackA=dir.resolve("b3-acked-a.log"), ackB=dir.resolve("b3-acked-b.log"), ok=dir.resolve("b3-ok.log");
        Server first=startServer(data); var pub=publish(first.port,1000,ok,"2");
        var a=consumer(first.port,aLog,ackA,"5"); awaitLineCount(ok,400);
        assertThat(pub.isAlive()).isTrue(); ps.destroyForcibly(first.child); assertNoLocks(data); assertConsumerExits(a);
        assertThat(pub.awaitExit(LIMIT)).isNotZero();
        int okCount=completeLines(ok); assertThat(okCount).isGreaterThanOrEqualTo(400);
        Server second=startServer(data); assertRecovery(second,okCount);
        var b=consumer(second.port,bLog,ackB,"5"); awaitDistinct(bLog,200,1000,b);
        assertThat(b.isAlive()).isTrue(); ps.destroyForcibly(second.child); assertNoLocks(data); assertConsumerExits(b);
        Server third=startServer(data); assertRecovery(third,okCount); consumer(third.port,cLog,null,null);
        var report=await(List.of(aLog,bLog,cLog),List.of(ackA,ackB),ok,okCount);
        assertVerified(report); assertThat(number(second.recovered,"messages")).isGreaterThanOrEqualTo(okCount);
        assertThat(number(third.recovered,"messages")).isGreaterThanOrEqualTo(okCount);
    }

    @Test void corruptMiddleRecordFailsWithoutChangingFilesAndTornTailStarts() throws Exception {
        Path corrupt=dir.resolve("corrupt-data"); Server first=startServer(corrupt);
        var pub=publish(first.port,5,dir.resolve("corrupt-ok.log"),null); assertThat(pub.awaitExit(LIMIT)).isZero();
        ps.destroyForcibly(first.child);
        Path segment=segmentFiles(corrupt).getFirst();
        try(RandomAccessFile file=new RandomAccessFile(segment.toFile(),"rw")) {
            file.seek(29); int original=file.read(); file.seek(29); file.write(original ^ 0x01);
        }
        Map<Path,byte[]> snapshot=snapshot(corrupt);
        ChaosProcesses.Child rejected=ps.startMain("dev.miniq.server.MiniQServerMain","--port","0","--data-dir",corrupt.toString(),"--ack-timeout-ms","500");
        assertThat(rejected.awaitExit(Duration.ofSeconds(10))).isEqualTo(1);
        assertThat(rejected.lines().toString()).doesNotContain("MINIQ_READY").doesNotContain("MINIQ_RECOVERED");
        assertSnapshotUnchanged(corrupt,snapshot);

        Path torn=dir.resolve("torn-data"); Server good=startServer(torn);
        var one=publish(good.port,3,dir.resolve("torn-ok.log"),null); assertThat(one.awaitExit(LIMIT)).isZero();
        ps.destroyForcibly(good.child); Path finalSegment=segmentFiles(torn).getFirst();
        try(FileChannel channel=FileChannel.open(finalSegment,java.nio.file.StandardOpenOption.WRITE)) {
            channel.truncate(Files.size(finalSegment)-2);
        }
        Server recovered=startServer(torn); assertThat(recovered.recovered).contains("messages=2");
        ps.destroyForcibly(recovered.child); assertNoLocks(torn);
        Server recoveredAgain=startServer(torn); assertThat(recoveredAgain.recovered).contains("messages=2");
    }

    @Test void combinedOutputKeepsStdoutAndStderrLineOrder() throws Exception {
        var fixture=ps.startMain("dev.miniq.chaos.CombinedStreamFixtureMain");
        var lines=fixture.waitForMarkersInOrder("FIXTURE_STDOUT_FIRST","FIXTURE_STDERR_SECOND",Duration.ofSeconds(10));
        assertThat(lines).containsExactly("FIXTURE_STDOUT_FIRST","FIXTURE_STDERR_SECOND");
        assertThat(fixture.awaitExit(Duration.ofSeconds(10))).isZero();
    }

    private record Server(ChaosProcesses.Child child,String port,String recovered) { }
    private Server startServer(Path data) throws Exception {
        return startServer(data,"default","default");
    }
    private Server startServer(Path data,String fsync,String maxInFlight) throws Exception {
        List<String> args=new ArrayList<>(List.of("--port","0","--data-dir",data.toString(),"--ack-timeout-ms","500"));
        if(!fsync.equals("default"))args.addAll(List.of("--fsync",fsync));
        if(!maxInFlight.equals("default"))args.addAll(List.of("--max-in-flight",maxInFlight));
        var c=ps.startMain("dev.miniq.server.MiniQServerMain",args.toArray(String[]::new));
        var markers=c.waitForMarkersInOrder("MINIQ_RECOVERED ","MINIQ_READY ",LIMIT);
        assertThat(markers.getFirst()).matches("MINIQ_RECOVERED topics=\\d+ messages=\\d+ acked=\\d+");
        var portMatcher=READY_PORT.matcher(markers.get(1)); assertThat(portMatcher.matches()).isTrue();
        assertThat(c.lines().toString()).doesNotContain("already open");
        return new Server(c,portMatcher.group(1),markers.getFirst());
    }

    private void assertRecovery(Server server,int minimumMessages) {
        assertThat(number(server.recovered,"messages")).isGreaterThanOrEqualTo(minimumMessages);
        assertThat(server.child.lines().toString()).doesNotContain("already open");
    }

    private ChaosProcesses.Child publish(String port,int count,Path ok,String delay) throws Exception {
        List<String> args=new ArrayList<>(List.of("--host","127.0.0.1","--port",port,"--topic","orders","--count",Integer.toString(count),"--ok-log",ok.toString()));
        if(delay!=null) args.addAll(List.of("--delay-ms",delay));
        var p=ps.startMain("dev.miniq.chaos.ChaosPublisherMain",args.toArray(String[]::new));
        if(delay==null) {
            assertThat(p.waitForMarker("CHAOS_PUBLISHED",LIMIT)).contains(Integer.toString(count));
            assertThat(p.awaitExit(LIMIT)).isZero();
        }
        return p;
    }

    private ChaosProcesses.Child consumer(String port,Path log,Path acked,String delay) throws Exception {
        List<String> args=new ArrayList<>(List.of("--host","127.0.0.1","--port",port,"--topic","orders","--log",log.toString(),"--ack-timeout-ms","500"));
        if(acked!=null) args.addAll(List.of("--acked-log",acked.toString()));
        if(delay!=null) args.addAll(List.of("--delay-ms",delay));
        var c=ps.startMain("dev.miniq.chaos.ChaosConsumerMain",args.toArray(String[]::new));
        assertThat(c.waitForMarker("CHAOS_CONSUMER_READY",LIMIT)).isEqualTo("CHAOS_CONSUMER_READY"); return c;
    }

    private void assertConsumerExits(ChaosProcesses.Child consumer) throws Exception {
        assertThat(consumer.awaitExit(Duration.ofSeconds(10))).isNotZero();
    }

    private void awaitLineCount(Path file,int count) throws Exception {
        long deadline=System.nanoTime()+LIMIT.toNanos();
        while(System.nanoTime()<deadline) { if(Files.exists(file)&&completeLines(file)>=count)return; Thread.onSpinWait(); }
        throw new AssertionError("Timed out waiting for "+count+" publish OK log lines; saw "+completeLines(file));
    }

    private void awaitDistinct(Path file,int count,int expected,ChaosProcesses.Child consumer) throws Exception {
        long deadline=System.nanoTime()+LIMIT.toNanos();
        while(System.nanoTime()<deadline) {
            if(Files.exists(file)&&ChaosVerifier.verify(file,expected).distinctIds()>=count)return;
            if(!consumer.isAlive()) throw new AssertionError("Consumer exited before "+count+" distinct receives. Output: "+consumer.lines());
            Thread.onSpinWait();
        }
        throw new AssertionError("Timed out waiting for "+count+" distinct receives. Consumer output: "+consumer.lines());
    }

    private StageBVerifier.Report await(List<Path> phases,List<Path> acks,Path ok,int expected) throws Exception {
        long deadline=System.nanoTime()+LIMIT.toNanos();
        while(System.nanoTime()<deadline) { var report=StageBVerifier.verify(phases,acks,ok,expected); if(report.publishedOk()>=expected&&report.missing().isEmpty())return report; Thread.onSpinWait(); }
        throw new AssertionError(StageBVerifier.verify(phases,acks,ok,expected).format());
    }

    private void assertVerified(StageBVerifier.Report report) {
        assertThat(report.passed()).isTrue(); assertThat(report.missing()).isEmpty(); assertThat(report.resurrected()).isEmpty();
        assertThat(report.noPostRestartWereSeenBefore()).isTrue();
        assertThat(report.receivedBeforeRestart()).containsAll(report.noPostRestart());
        assertThat(report.postRestartMinimum()).allSatisfy((id,count)->assertThat(count).isEqualTo(1));
        System.out.println(report.format());
    }

    private static int completeLines(Path file) throws Exception {
        if(!Files.exists(file))return 0; byte[] b=Files.readAllBytes(file); int n=0;
        for(byte x:b)if(x=='\n')n++; return n;
    }

    private static void assertNoLocks(Path data) throws Exception {
        try(var stream=Files.walk(data)) { assertThat(stream.filter(p->p.getFileName().toString().toLowerCase().contains("lock")).toList()).isEmpty(); }
    }

    private static List<Path> segmentFiles(Path data) throws Exception {
        try(var stream=Files.walk(data)) { return stream.filter(Files::isRegularFile).filter(p->p.getFileName().toString().endsWith(".log")).toList(); }
    }

    private static Map<Path,byte[]> snapshot(Path root) throws Exception {
        Map<Path,byte[]> files=new HashMap<>();
        try(var stream=Files.walk(root)) { for(Path p:stream.filter(Files::isRegularFile).toList())files.put(root.relativize(p),Files.readAllBytes(p)); }
        return files;
    }

    private static void assertSnapshotUnchanged(Path root,Map<Path,byte[]> expected) throws Exception {
        Map<Path,byte[]> actual=snapshot(root); assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
        expected.forEach((name,bytes)->assertThat(actual.get(name)).as(name.toString()).containsExactly(bytes));
    }

    private static long number(String line,String key) {
        var matcher=Pattern.compile(key+"=(\\d+)").matcher(line); if(!matcher.find())throw new AssertionError(line); return Long.parseLong(matcher.group(1));
    }
}
