package org.netpreserve.warclens;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.netpreserve.jwarc.HttpResponse;
import org.netpreserve.jwarc.WarcResponse;
import org.netpreserve.jwarc.WarcWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MainTest {
    @Test
    void initCreatesDatabase(@TempDir Path tempDir) {
        RunResult result = runFromWorkingDir(tempDir, "init");

        assertEquals(0, result.exitCode);
        assertTrue(Files.exists(tempDir.resolve("warclens.duckdb")));
        assertTrue(result.stdout.contains("Initialized warclens.duckdb"));
    }

    @Test
    void initWithWarcsAndHostsReport(@TempDir Path tempDir) throws IOException {
        Path warcPath = tempDir.resolve("sample.warc.gz");
        writeSampleWarc(warcPath);

        RunResult initResult = runFromWorkingDir(tempDir, "init", warcPath.toString());
        assertEquals(0, initResult.exitCode);
        assertTrue(initResult.stdout.contains("Initialized warclens.duckdb"));
        assertTrue(initResult.stdout.contains("Imported 3 records."));

        RunResult hostsResult = runFromWorkingDir(tempDir, "hosts");
        assertEquals(0, hostsResult.exitCode);
        assertTrue(hostsResult.stdout.contains("HOST"));
        assertTrue(hostsResult.stdout.contains("PAGES"));
        assertTrue(hostsResult.stdout.contains("SIZE"));
        assertTrue(hostsResult.stdout.matches("(?s).*example.org\\s+2\\s+1\\s+\\S+\\s+B.*"));
        assertTrue(hostsResult.stdout.matches("(?s).*other.example\\s+1\\s+1\\s+\\S+\\s+B.*"));
        assertTrue(hostsResult.stdout.indexOf("example.org") < hostsResult.stdout.indexOf("other.example"));
    }

    @Test
    void initStoresWarcOffsets(@TempDir Path tempDir) throws Exception {
        Path warcPath = tempDir.resolve("sample.warc.gz");
        writeSampleWarc(warcPath);

        assertEquals(0, runFromWorkingDir(tempDir, "init", warcPath.toString()).exitCode);

        Path dbPath = tempDir.resolve("warclens.duckdb");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath.toAbsolutePath());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS c FROM records WHERE warc_offset IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("c"));
        }
    }

    @Test
    void initWithWarcsAndMediaTypesReport(@TempDir Path tempDir) throws IOException {
        Path warcPath = tempDir.resolve("sample.warc.gz");
        writeSampleWarc(warcPath);

        assertEquals(0, runFromWorkingDir(tempDir, "init", warcPath.toString()).exitCode);

        RunResult mediaResult = runFromWorkingDir(tempDir, "mime");
        assertEquals(0, mediaResult.exitCode);
        assertTrue(mediaResult.stdout.contains("MEDIA-TYPE"));
        assertTrue(mediaResult.stdout.contains("REC%"));
        assertTrue(mediaResult.stdout.contains("SIZE%"));
        assertTrue(!mediaResult.stdout.contains("PAGES"));
        assertTrue(mediaResult.stdout.matches("(?s).*text/html\\s+2\\s+\\d+(?:\\.\\d)?%\\s+\\S+\\s+B\\s+\\d+(?:\\.\\d)?%.*"));
        assertTrue(mediaResult.stdout.matches("(?s).*text/plain\\s+1\\s+\\d+(?:\\.\\d)?%\\s+\\S+\\s+B\\s+\\d+(?:\\.\\d)?%.*"));
        assertTrue(mediaResult.stdout.matches("(?s).*TOTAL\\s+3\\s+100%\\s+\\S+\\s+B\\s+100%.*"));
        assertTrue(mediaResult.stdout.indexOf("text/html") < mediaResult.stdout.indexOf("text/plain")); // sorted by size
    }

    @Test
    void mediaTypesReportSupportsHostAndSiteFilters(@TempDir Path tempDir) throws IOException {
        Path warcPath = tempDir.resolve("filter-sample.warc.gz");
        try (WarcWriter writer = new WarcWriter(warcPath)) {
            writer.write(buildResponse("https://facebook.com/home", "text/html", "<html>home</html>"));
            writer.write(buildResponse("https://connect.facebook.com/script.js", "application/javascript;charset=utf8", "alert(1)"));
            writer.write(buildResponse("https://a.b.c.d.facebook.com/pixel.png", "image/png", "PNGDATA"));
            writer.write(buildResponse("https://example.com/other", "text/plain", "other"));
        }

        assertEquals(0, runFromWorkingDir(tempDir, "init", warcPath.toString()).exitCode);

        RunResult siteResult = runFromWorkingDir(tempDir, "mime", "--site", "facebook.com");
        assertEquals(0, siteResult.exitCode);
        assertTrue(siteResult.stdout.contains("text/html"));
        assertTrue(siteResult.stdout.contains("application/javascript"));
        assertTrue(siteResult.stdout.contains("image/png"));
        assertTrue(!siteResult.stdout.contains("text/plain"));
        assertTrue(siteResult.stdout.matches("(?s).*TOTAL\\s+3\\s+100%\\s+\\S+\\s+[BKMGTPE]\\s+100%.*"));

        RunResult hostResult = runFromWorkingDir(tempDir, "mime", "--host", "connect.facebook.com");
        assertEquals(0, hostResult.exitCode);
        assertTrue(hostResult.stdout.contains("application/javascript"));
        assertTrue(!hostResult.stdout.contains("text/html"));
        assertTrue(!hostResult.stdout.contains("image/png"));
        assertTrue(hostResult.stdout.matches("(?s).*TOTAL\\s+1\\s+100%\\s+\\S+\\s+[BKMGTPE]\\s+100%.*"));
    }

    @Test
    void hostsReportSupportsSiteFilter(@TempDir Path tempDir) throws IOException {
        Path warcPath = tempDir.resolve("hosts-filter-sample.warc.gz");
        try (WarcWriter writer = new WarcWriter(warcPath)) {
            writer.write(buildResponse("https://facebook.com/home", "text/html", "<html>home</html>"));
            writer.write(buildResponse("https://connect.facebook.com/script.js", "application/javascript", "alert(1)"));
            writer.write(buildResponse("https://a.b.c.d.facebook.com/pixel.png", "image/png", "PNGDATA"));
            writer.write(buildResponse("https://example.com/other", "text/plain", "other"));
        }

        assertEquals(0, runFromWorkingDir(tempDir, "init", warcPath.toString()).exitCode);

        RunResult siteHosts = runFromWorkingDir(tempDir, "hosts", "--site", "facebook.com");
        assertEquals(0, siteHosts.exitCode);
        assertTrue(siteHosts.stdout.contains("facebook.com"));
        assertTrue(siteHosts.stdout.contains("connect.facebook.com"));
        assertTrue(siteHosts.stdout.contains("a.b.c.d.facebook.com"));
        assertTrue(!siteHosts.stdout.contains("example.com"));
    }

    @Test
    void statusCodesReportAndFilters(@TempDir Path tempDir) throws IOException {
        Path warcPath = tempDir.resolve("status-sample.warc.gz");
        try (WarcWriter writer = new WarcWriter(warcPath)) {
            writer.write(buildResponse("https://facebook.com/home", 200, "text/html", "<html>home</html>"));
            writer.write(buildResponse("https://connect.facebook.com/script.js", 200, "application/javascript", "alert(1)"));
            writer.write(buildResponse("https://a.b.c.d.facebook.com/notfound", 404, "text/html", "<html>missing</html>"));
            writer.write(buildResponse("https://example.com/error", 500, "text/plain", "err"));
        }

        assertEquals(0, runFromWorkingDir(tempDir, "init", warcPath.toString()).exitCode);

        RunResult all = runFromWorkingDir(tempDir, "status");
        assertEquals(0, all.exitCode);
        assertTrue(all.stdout.contains("STATUS"));
        assertTrue(all.stdout.contains("200"));
        assertTrue(all.stdout.contains("404"));
        assertTrue(all.stdout.contains("500"));
        assertTrue(all.stdout.matches("(?s).*TOTAL\\s+4\\s+100%\\s+\\S+\\s+[BKMGTPE]\\s+100%.*"));

        RunResult site = runFromWorkingDir(tempDir, "status", "--site", "facebook.com");
        assertEquals(0, site.exitCode);
        assertTrue(site.stdout.contains("200"));
        assertTrue(site.stdout.contains("404"));
        assertTrue(!site.stdout.contains("500"));
        assertTrue(site.stdout.matches("(?s).*TOTAL\\s+3\\s+100%\\s+\\S+\\s+[BKMGTPE]\\s+100%.*"));

        RunResult host = runFromWorkingDir(tempDir, "status", "--host", "connect.facebook.com");
        assertEquals(0, host.exitCode);
        assertTrue(host.stdout.contains("200"));
        assertTrue(!host.stdout.contains("404"));
        assertTrue(!host.stdout.contains("500"));
        assertTrue(host.stdout.matches("(?s).*TOTAL\\s+1\\s+100%\\s+\\S+\\s+[BKMGTPE]\\s+100%.*"));

        RunResult statusFiltered = runFromWorkingDir(tempDir, "status", "--status", "404");
        assertEquals(0, statusFiltered.exitCode);
        assertTrue(statusFiltered.stdout.matches("(?s).*\\n404\\s+1\\s+100%\\s+\\S+\\s+[BKMGTPE]\\s+100%.*"));
        assertTrue(!containsStatusLine(statusFiltered.stdout, 200));
        assertTrue(!containsStatusLine(statusFiltered.stdout, 500));

        RunResult status4xx = runFromWorkingDir(tempDir, "status", "--status", "4xx");
        assertEquals(0, status4xx.exitCode);
        assertTrue(containsStatusLine(status4xx.stdout, 404));
        assertTrue(!containsStatusLine(status4xx.stdout, 200));
        assertTrue(!containsStatusLine(status4xx.stdout, 500));

        RunResult status5xx = runFromWorkingDir(tempDir, "status", "--status", "5xx");
        assertEquals(0, status5xx.exitCode);
        assertTrue(containsStatusLine(status5xx.stdout, 500));
        assertTrue(!containsStatusLine(status5xx.stdout, 200));
        assertTrue(!containsStatusLine(status5xx.stdout, 404));

        RunResult statusUnion = runFromWorkingDir(tempDir, "status", "--status", "4xx", "--status", "5xx", "--status", "200");
        assertEquals(0, statusUnion.exitCode);
        assertTrue(containsStatusLine(statusUnion.stdout, 200));
        assertTrue(containsStatusLine(statusUnion.stdout, 404));
        assertTrue(containsStatusLine(statusUnion.stdout, 500));
        assertTrue(statusUnion.stdout.matches("(?s).*TOTAL\\s+4\\s+100%\\s+\\S+\\s+[BKMGTPE]\\s+100%.*"));

        RunResult mimeFiltered = runFromWorkingDir(tempDir, "mime", "--status", "404");
        assertEquals(0, mimeFiltered.exitCode);
        assertTrue(mimeFiltered.stdout.contains("text/html"));
        assertTrue(!mimeFiltered.stdout.contains("application/javascript"));
        assertTrue(!mimeFiltered.stdout.contains("text/plain"));
        assertTrue(mimeFiltered.stdout.matches("(?s).*TOTAL\\s+1\\s+100%\\s+\\S+\\s+[BKMGTPE]\\s+100%.*"));

        RunResult hostsFiltered = runFromWorkingDir(tempDir, "hosts", "--status", "500");
        assertEquals(0, hostsFiltered.exitCode);
        assertTrue(hostsFiltered.stdout.contains("example.com"));
        assertTrue(!hostsFiltered.stdout.contains("facebook.com"));
        assertTrue(!hostsFiltered.stdout.contains("connect.facebook.com"));

        RunResult hostDomainUnion = runFromWorkingDir(
                tempDir,
                "status",
                "--host", "connect.facebook.com",
                "--domain", "example.com"
        );
        assertEquals(0, hostDomainUnion.exitCode);
        assertTrue(containsStatusLine(hostDomainUnion.stdout, 200));
        assertTrue(containsStatusLine(hostDomainUnion.stdout, 500));
        assertTrue(!containsStatusLine(hostDomainUnion.stdout, 404));
    }

    @Test
    void importCommandNoLongerSupported(@TempDir Path tempDir) {
        RunResult result = runFromWorkingDir(tempDir, "import");
        assertEquals(1, result.exitCode);
        assertTrue(result.stderr.contains("Unknown command: import"));
    }

    private static void writeSampleWarc(Path path) throws IOException {
        try (WarcWriter writer = new WarcWriter(path)) {
            writer.write(buildResponse("https://example.org/page-1", "text/html", "<html>first</html>"));
            writer.write(buildResponse("https://example.org/page-2", "text/plain", "second"));
            writer.write(buildResponse("https://other.example/page", "text/html; charset=utf-8", "<html>third</html>"));
        }
    }

    private static WarcResponse buildResponse(String url, String contentType, String bodyText) throws IOException {
        return buildResponse(url, 200, contentType, bodyText);
    }

    private static WarcResponse buildResponse(String url, int status, String contentType, String bodyText) throws IOException {
        HttpResponse httpResponse = new HttpResponse.Builder(status, "OK")
                .setHeader("Content-Type", contentType)
                .body(null, bodyText.getBytes(StandardCharsets.UTF_8))
                .build();
        return new WarcResponse.Builder(url).body(httpResponse).build();
    }

    private static RunResult runFromWorkingDir(Path workingDir, String... args) {
        synchronized (MainTest.class) {
            String oldUserDir = System.getProperty("user.dir");
            System.setProperty("user.dir", workingDir.toString());
            try {
                ByteArrayOutputStream stdoutBuffer = new ByteArrayOutputStream();
                ByteArrayOutputStream stderrBuffer = new ByteArrayOutputStream();
                PrintStream out = new PrintStream(stdoutBuffer, true, StandardCharsets.UTF_8);
                PrintStream err = new PrintStream(stderrBuffer, true, StandardCharsets.UTF_8);
                int exitCode = Main.run(args, out, err);
                return new RunResult(
                        exitCode,
                        stdoutBuffer.toString(StandardCharsets.UTF_8),
                        stderrBuffer.toString(StandardCharsets.UTF_8)
                );
            } finally {
                System.setProperty("user.dir", oldUserDir);
            }
        }
    }

    private static boolean containsStatusLine(String output, int statusCode) {
        String prefix = Integer.toString(statusCode) + " ";
        return output.lines().anyMatch(line -> line.stripLeading().startsWith(prefix));
    }

    private record RunResult(int exitCode, String stdout, String stderr) {
    }
}
