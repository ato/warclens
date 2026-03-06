package org.netpreserve.warclens;

import org.netpreserve.jwarc.HttpResponse;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcResponse;
import org.netpreserve.jwarc.WarcTargetRecord;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {
    private static final String DB_FILENAME = "warclens.duckdb";
    private static final int IMPORT_BATCH_SIZE = 10_000;
    private static final String INSERT_SQL =
            "INSERT INTO records (url, host, status, mime, size_bytes, warc_offset, warc_type, fetch_time, warc_filename) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || isHelp(args[0])) {
            printUsage(out);
            return args.length == 0 ? 1 : 0;
        }

        try {
            return switch (args[0]) {
                case "init" -> init(sliceArgs(args, 1), out, err);
                case "hosts" -> hostsReport(sliceArgs(args, 1), out, err);
                case "mime", "media-types", "media" -> mediaTypesReport(sliceArgs(args, 1), out, err);
                case "status", "status-codes" -> statusCodesReport(sliceArgs(args, 1), out, err);
                default -> {
                    err.println("Unknown command: " + args[0]);
                    printUsage(out);
                    yield 1;
                }
            };
        } catch (Exception e) {
            err.println("Error: " + e.getMessage());
            e.printStackTrace(err);
            return 1;
        }
    }

    private static int init(String[] warcFiles, PrintStream out, PrintStream err) throws SQLException, IOException {
        initializeSchema();
        out.println("Initialized " + DB_FILENAME);
        if (warcFiles.length > 0) {
            return importWarcs(warcFiles, out, err);
        }
        return 0;
    }

    private static void initializeSchema() throws SQLException {
        try (Connection conn = connect(true); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS records (" +
                    "url VARCHAR, " +
                    "host VARCHAR, " +
                    "status INTEGER, " +
                    "mime VARCHAR, " +
                    "size_bytes BIGINT, " +
                    "warc_offset BIGINT, " +
                    "warc_type VARCHAR, " +
                    "fetch_time TIMESTAMP, " +
                    "warc_filename VARCHAR" +
                    ")");
            stmt.execute("ALTER TABLE records ADD COLUMN IF NOT EXISTS size_bytes BIGINT");
            stmt.execute("ALTER TABLE records ADD COLUMN IF NOT EXISTS warc_offset BIGINT");
        }
    }

    private static int importWarcs(String[] files, PrintStream out, PrintStream err) throws IOException, SQLException {
        if (files.length == 0) {
            return 0;
        }

        Path dbPath = databasePath();
        if (!Files.exists(dbPath)) {
            err.println("Database not found: " + DB_FILENAME + ". Run 'warclens init' first.");
            return 1;
        }

        List<Path> warcPaths = new ArrayList<>();
        for (String file : files) {
            warcPaths.add(Paths.get(file));
        }
        for (Path path : warcPaths) {
            if (!Files.exists(path)) {
                throw new IOException("File not found: " + path);
            }
        }

        int parallelism = Math.max(1, Math.min(warcPaths.size(), Runtime.getRuntime().availableProcessors()));
        if (parallelism == 1) {
            long total = 0;
            for (Path path : warcPaths) {
                total += importSingleWarc(path);
            }
            out.println("Imported " + total + " records.");
            return 0;
        }

        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        try {
            List<Future<Long>> futures = new ArrayList<>();
            for (Path path : warcPaths) {
                futures.add(pool.submit(new ImportTask(path)));
            }
            long total = 0;
            for (Future<Long> future : futures) {
                try {
                    total += future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Import interrupted", e);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof IOException ioException) {
                        throw ioException;
                    }
                    if (cause instanceof SQLException sqlException) {
                        throw sqlException;
                    }
                    if (cause instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    }
                    throw new IOException("Import failed", cause);
                }
            }
            out.println("Imported " + total + " records.");
        } finally {
            pool.shutdownNow();
        }
        return 0;
    }

    private static long importSingleWarc(Path path) throws IOException, SQLException {
        try (Connection conn = connect(false)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                long total = importWarc(path, ps);
                conn.commit();
                return total;
            } catch (IOException | RuntimeException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    private static long importWarc(Path path, PreparedStatement ps) throws IOException, SQLException {
        if (!Files.exists(path)) {
            throw new IOException("File not found: " + path);
        }

        long count = 0;
        int pendingBatchRows = 0;
        try (WarcReader reader = new WarcReader(path)) {
            for (WarcRecord record : reader) {
                String url = null;
                String host = null;
                if (record instanceof WarcTargetRecord targetRecord) {
                    url = targetRecord.target();
                    try {
                        host = targetRecord.targetURI().getHost();
                    } catch (IllegalArgumentException e) {
                        host = null;
                    }
                }

                Integer status = null;
                String mime = null;
                Long sizeBytes = null;
                if (record instanceof WarcResponse response) {
                    try {
                        HttpResponse http = response.http();
                        status = http.status();
                        mime = http.contentType().base().toString();
                        sizeBytes = http.body().size();
                    } catch (IOException e) {
                        status = null;
                        mime = null;
                        sizeBytes = null;
                    }
                }

                String warcType = record.type();
                Instant instant = record.date();
                Timestamp fetchTime = instant == null ? null : Timestamp.from(instant);
                Long warcOffset;
                try {
                    warcOffset = record.position();
                } catch (UnsupportedOperationException e) {
                    warcOffset = null;
                }
                String filename = path.getFileName().toString();

                ps.setString(1, url);
                ps.setString(2, host);
                if (status == null) {
                    ps.setNull(3, java.sql.Types.INTEGER);
                } else {
                    ps.setInt(3, status);
                }
                ps.setString(4, mime);
                if (sizeBytes == null) {
                    ps.setNull(5, java.sql.Types.BIGINT);
                } else {
                    ps.setLong(5, sizeBytes);
                }
                if (warcOffset == null) {
                    ps.setNull(6, java.sql.Types.BIGINT);
                } else {
                    ps.setLong(6, warcOffset);
                }
                ps.setString(7, warcType);
                if (fetchTime == null) {
                    ps.setNull(8, java.sql.Types.TIMESTAMP);
                } else {
                    ps.setTimestamp(8, fetchTime);
                }
                ps.setString(9, filename);
                ps.addBatch();
                count++;
                pendingBatchRows++;
                if (pendingBatchRows >= IMPORT_BATCH_SIZE) {
                    ps.executeBatch();
                    pendingBatchRows = 0;
                }
            }
            if (pendingBatchRows > 0) {
                ps.executeBatch();
            }
        }
        return count;
    }

    private static int hostsReport(String[] args, PrintStream out, PrintStream err) throws SQLException {
        ReportFilter filter = parseReportFilter(args, out, err, "hosts", true, true, true);
        if (filter == null) {
            return 1;
        }

        Path dbPath = databasePath();
        if (!Files.exists(dbPath)) {
            err.println("Database not found: " + DB_FILENAME + ". Run 'warclens init' first.");
            return 1;
        }

        String query = "SELECT host, COUNT(*) AS records, " +
                "SUM(CASE WHEN lower(mime) = 'text/html' THEN 1 ELSE 0 END) AS pages, " +
                "COALESCE(SUM(size_bytes), 0) AS size_bytes " +
                "FROM records " +
                "WHERE host IS NOT NULL";
        query += buildReportFilterClause(filter, " AND ");
        query += " GROUP BY host ORDER BY records DESC, host ASC";

        try (Connection conn = connect(false);
             PreparedStatement ps = conn.prepareStatement(query)) {
            bindReportFilter(ps, filter, 1);
            try (ResultSet rs = ps.executeQuery()) {
                int rows = 0;
                out.printf("%-40s %10s %10s %12s%n", "HOST", "RECORDS", "PAGES", "SIZE");
                while (rs.next()) {
                    String host = rs.getString("host");
                    long records = rs.getLong("records");
                    long pages = rs.getLong("pages");
                    long sizeBytes = rs.getLong("size_bytes");
                    out.printf("%-40s %10d %10d %12s%n", host, records, pages, humanReadableBytes(sizeBytes));
                    rows++;
                }
                if (rows == 0) {
                    out.println("(no host data)");
                }
            }
        }
        return 0;
    }

    private static int mediaTypesReport(String[] args, PrintStream out, PrintStream err) throws SQLException {
        ReportFilter filter = parseReportFilter(args, out, err, "mime", true, true, true);
        if (filter == null) {
            return 1;
        }

        Path dbPath = databasePath();
        if (!Files.exists(dbPath)) {
            err.println("Database not found: " + DB_FILENAME + ". Run 'warclens init' first.");
            return 1;
        }

        String query =
                     "WITH normalized AS (" +
                             "  SELECT " +
                             "    CASE " +
                             "      WHEN mime IS NULL THEN NULL " +
                             "      WHEN lower(trim(split_part(mime, ';', 1))) IN ('text/javascript', 'application/x-javascript') THEN 'application/javascript' " +
                             "      ELSE lower(trim(split_part(mime, ';', 1))) " +
                             "    END AS media_type, " +
                             "    COALESCE(size_bytes, 0) AS size_bytes " +
                             "  FROM records" + buildReportFilterClause(filter, " WHERE ") +
                             "), agg AS (" +
                             "  SELECT media_type, COUNT(*) AS records, " +
                             "         SUM(size_bytes) AS size_bytes " +
                             "  FROM normalized " +
                             "  WHERE media_type IS NOT NULL " +
                             "  GROUP BY media_type" +
                             "), totals AS (" +
                             "  SELECT COALESCE(SUM(records), 0) AS total_records, " +
                             "         COALESCE(SUM(size_bytes), 0) AS total_size " +
                             "  FROM agg" +
                             ") " +
                             "SELECT a.media_type, a.records, a.size_bytes, t.total_records, t.total_size " +
                             "FROM agg a " +
                             "CROSS JOIN totals t " +
                             "ORDER BY a.size_bytes DESC, a.records DESC, a.media_type ASC";

        try (Connection conn = connect(false);
             PreparedStatement ps = conn.prepareStatement(query)) {
            bindReportFilter(ps, filter, 1);
            try (ResultSet rs = ps.executeQuery()) {
                int rows = 0;
                long totalRecords = 0;
                long totalSize = 0;
                out.printf("%-30s %10s %8s %12s %8s%n", "MEDIA-TYPE", "RECORDS", "REC%", "SIZE", "SIZE%");
                while (rs.next()) {
                    String mime = rs.getString("media_type");
                    long records = rs.getLong("records");
                    long sizeBytes = rs.getLong("size_bytes");
                    totalRecords = rs.getLong("total_records");
                    totalSize = rs.getLong("total_size");
                    out.printf("%-30s %10d %8s %12s %8s%n",
                            mime,
                            records,
                            formatPercent(records, totalRecords),
                            humanReadableBytes(sizeBytes),
                            formatPercent(sizeBytes, totalSize));
                    rows++;
                }
                if (rows == 0) {
                    out.println("(no media type data)");
                } else {
                    out.printf("%-30s %10d %8s %12s %8s%n",
                            "TOTAL",
                            totalRecords,
                            "100%",
                            humanReadableBytes(totalSize),
                            "100%");
                }
            }
        }
        return 0;
    }

    private static int statusCodesReport(String[] args, PrintStream out, PrintStream err) throws SQLException {
        ReportFilter filter = parseReportFilter(args, out, err, "status", true, true, true);
        if (filter == null) {
            return 1;
        }

        Path dbPath = databasePath();
        if (!Files.exists(dbPath)) {
            err.println("Database not found: " + DB_FILENAME + ". Run 'warclens init' first.");
            return 1;
        }

        String query =
                "WITH normalized AS (" +
                        "  SELECT status, COALESCE(size_bytes, 0) AS size_bytes " +
                        "  FROM records" + buildReportFilterClause(filter, " WHERE ") +
                        "), agg AS (" +
                        "  SELECT status, COUNT(*) AS records, SUM(size_bytes) AS size_bytes " +
                        "  FROM normalized " +
                        "  WHERE status IS NOT NULL " +
                        "  GROUP BY status" +
                        "), totals AS (" +
                        "  SELECT COALESCE(SUM(records), 0) AS total_records, " +
                        "         COALESCE(SUM(size_bytes), 0) AS total_size " +
                        "  FROM agg" +
                        ") " +
                        "SELECT a.status, a.records, a.size_bytes, t.total_records, t.total_size " +
                        "FROM agg a " +
                        "CROSS JOIN totals t " +
                        "ORDER BY a.records DESC, a.status ASC";

        try (Connection conn = connect(false);
             PreparedStatement ps = conn.prepareStatement(query)) {
            bindReportFilter(ps, filter, 1);
            try (ResultSet rs = ps.executeQuery()) {
                int rows = 0;
                long totalRecords = 0;
                long totalSize = 0;
                out.printf("%-12s %10s %8s %12s %8s%n", "STATUS", "RECORDS", "REC%", "SIZE", "SIZE%");
                while (rs.next()) {
                    int status = rs.getInt("status");
                    long records = rs.getLong("records");
                    long sizeBytes = rs.getLong("size_bytes");
                    totalRecords = rs.getLong("total_records");
                    totalSize = rs.getLong("total_size");
                    out.printf("%-12d %10d %8s %12s %8s%n",
                            status,
                            records,
                            formatPercent(records, totalRecords),
                            humanReadableBytes(sizeBytes),
                            formatPercent(sizeBytes, totalSize));
                    rows++;
                }
                if (rows == 0) {
                    out.println("(no status code data)");
                } else {
                    out.printf("%-12s %10d %8s %12s %8s%n",
                            "TOTAL",
                            totalRecords,
                            "100%",
                            humanReadableBytes(totalSize),
                            "100%");
                }
            }
        }
        return 0;
    }

    private static String buildReportFilterClause(ReportFilter filter, String prefix) {
        StringBuilder clause = new StringBuilder();
        boolean hasCondition = false;
        if (!filter.hosts.isEmpty() || !filter.domains.isEmpty()) {
            StringJoiner hostJoiner = new StringJoiner(" OR ");
            for (int i = 0; i < filter.hosts.size(); i++) {
                hostJoiner.add("lower(host) = ?");
            }
            for (int i = 0; i < filter.domains.size(); i++) {
                hostJoiner.add("lower(host) = ?");
                hostJoiner.add("lower(host) LIKE ?");
            }
            clause.append(prefix).append("(").append(hostJoiner).append(")");
            hasCondition = true;
        }
        if (!filter.statusRanges.isEmpty()) {
            StringJoiner joiner = new StringJoiner(" OR ");
            for (int i = 0; i < filter.statusRanges.size(); i++) {
                joiner.add("status BETWEEN ? AND ?");
            }
            clause.append(hasCondition ? " AND " : prefix).append("(").append(joiner).append(")");
        }
        return clause.toString();
    }

    private static int bindReportFilter(PreparedStatement ps, ReportFilter filter, int startIndex) throws SQLException {
        int index = startIndex;
        for (String host : filter.hosts) {
            ps.setString(index++, host);
        }
        for (String domain : filter.domains) {
            ps.setString(index++, domain);
            ps.setString(index++, "%." + domain);
        }
        for (StatusRange range : filter.statusRanges) {
            ps.setInt(index++, range.low);
            ps.setInt(index++, range.high);
        }
        return index;
    }

    private static ReportFilter parseReportFilter(
            String[] args,
            PrintStream out,
            PrintStream err,
            String command,
            boolean allowHost,
            boolean allowSite,
            boolean allowStatus
    ) {
        List<String> hosts = new ArrayList<>();
        List<String> domains = new ArrayList<>();
        List<StatusRange> statusRanges = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--host" -> {
                    if (!allowHost) {
                        err.println("Unknown option: " + arg);
                        printReportUsage(out, command, allowHost, allowSite, allowStatus);
                        return null;
                    }
                    if (i + 1 >= args.length) {
                        err.println("Missing value for --host");
                        printReportUsage(out, command, allowHost, allowSite, allowStatus);
                        return null;
                    }
                    hosts.add(normalizeDomain(args[++i]));
                }
                case "--site", "--domain" -> {
                    if (!allowSite) {
                        err.println("Unknown option: " + arg);
                        printReportUsage(out, command, allowHost, allowSite, allowStatus);
                        return null;
                    }
                    if (i + 1 >= args.length) {
                        err.println("Missing value for " + arg);
                        printReportUsage(out, command, allowHost, allowSite, allowStatus);
                        return null;
                    }
                    domains.add(normalizeDomain(args[++i]));
                }
                case "--status" -> {
                    if (!allowStatus) {
                        err.println("Unknown option: " + arg);
                        printReportUsage(out, command, allowHost, allowSite, allowStatus);
                        return null;
                    }
                    if (i + 1 >= args.length) {
                        err.println("Missing value for --status");
                        printReportUsage(out, command, allowHost, allowSite, allowStatus);
                        return null;
                    }
                    try {
                        StatusFilter parsed = parseStatusFilterArg(args[++i]);
                        statusRanges.add(new StatusRange(parsed.low, parsed.high));
                    } catch (IllegalArgumentException e) {
                        err.println(e.getMessage());
                        printReportUsage(out, command, allowHost, allowSite, allowStatus);
                        return null;
                    }
                }
                default -> {
                    err.println("Unknown option: " + arg);
                    printReportUsage(out, command, allowHost, allowSite, allowStatus);
                    return null;
                }
            }
        }

        return new ReportFilter(hosts, domains, statusRanges);
    }

    private static StatusFilter parseStatusFilterArg(String rawValue) {
        String value = Objects.requireNonNull(rawValue, "rawValue").trim().toLowerCase(Locale.ROOT);
        if (value.matches("[0-9]xx")) {
            int clazz = value.charAt(0) - '0';
            int low = clazz * 100;
            return new StatusFilter(low, low + 99);
        }
        try {
            int exact = Integer.parseInt(value);
            return new StatusFilter(exact, exact);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid status code: " + rawValue);
        }
    }

    private static Connection connect(boolean create) throws SQLException {
        String url = "jdbc:duckdb:" + databasePath().toAbsolutePath();
        Connection conn = DriverManager.getConnection(url);
        if (create) {
            conn.createStatement().execute("PRAGMA enable_object_cache");
        }
        return conn;
    }

    private static Path databasePath() {
        return Paths.get(System.getProperty("user.dir")).resolve(DB_FILENAME);
    }

    private static String normalizeDomain(String value) {
        String domain = Objects.requireNonNull(value, "value").trim().toLowerCase(Locale.ROOT);
        while (domain.endsWith(".")) {
            domain = domain.substring(0, domain.length() - 1);
        }
        return domain;
    }

    private static String humanReadableBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        String[] units = {"K", "M", "G", "T", "P"};
        double value = bytes;
        int unitIndex = -1;
        while (value >= 1024 && unitIndex < units.length - 1) {
            value /= 1024;
            unitIndex++;
        }
        if (value >= 10) {
            return String.format(Locale.ROOT, "%.0f %s", value, units[unitIndex]);
        }
        return String.format(Locale.ROOT, "%.1f %s", value, units[unitIndex]);
    }

    private static String formatPercent(long part, long total) {
        if (total <= 0) {
            return "0.0%";
        }
        double pct = (part * 100.0) / total;
        if (pct >= 10.0) {
            return String.format(Locale.ROOT, "%.0f%%", pct);
        }
        return String.format(Locale.ROOT, "%.1f%%", pct);
    }

    private static boolean isHelp(String arg) {
        return "-h".equals(arg) || "--help".equals(arg) || "help".equals(arg);
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage:");
        out.println("  warclens init [warc files]");
        out.println("  warclens hosts [--host HOST]... [--domain DOMAIN]... [--status CODE|Nxx]...");
        printMediaTypesUsage(out);
        printStatusCodesUsage(out);
    }

    private static void printMediaTypesUsage(PrintStream out) {
        out.println("  warclens mime [--host HOST]... [--domain DOMAIN]... [--status CODE|Nxx]...");
    }

    private static void printStatusCodesUsage(PrintStream out) {
        out.println("  warclens status [--host HOST]... [--domain DOMAIN]... [--status CODE|Nxx]...");
    }

    private static void printReportUsage(PrintStream out, String command, boolean allowHost, boolean allowSite, boolean allowStatus) {
        StringBuilder usage = new StringBuilder("  warclens ").append(command);
        if (allowHost && allowSite) {
            usage.append(" [--host HOST]... [--domain DOMAIN]...");
        } else if (allowSite) {
            usage.append(" [--domain DOMAIN]...");
        } else if (allowHost) {
            usage.append(" [--host HOST]...");
        }
        if (allowStatus) {
            usage.append(" [--status CODE|Nxx]...");
        }
        out.println(usage);
    }

    private static String[] sliceArgs(String[] args, int start) {
        if (start >= args.length) return new String[0];
        String[] slice = new String[args.length - start];
        System.arraycopy(args, start, slice, 0, slice.length);
        return slice;
    }

    private static final class ImportTask implements Callable<Long> {
        private final Path path;

        private ImportTask(Path path) {
            this.path = path;
        }

        @Override
        public Long call() throws Exception {
            return importSingleWarc(path);
        }
    }

    private record ReportFilter(List<String> hosts, List<String> domains, List<StatusRange> statusRanges) {
    }

    private record StatusFilter(int low, int high) {
    }

    private record StatusRange(int low, int high) {
    }
}
