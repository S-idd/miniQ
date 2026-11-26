package dev.miniq.chaos;

public final class CombinedStreamFixtureMain {
    private CombinedStreamFixtureMain() { }
    public static void main(String[] args) {
        System.out.println("FIXTURE_STDOUT_FIRST"); System.out.flush();
        System.err.println("FIXTURE_STDERR_SECOND"); System.err.flush();
    }
}
