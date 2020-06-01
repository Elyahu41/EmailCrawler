import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    static int EMAIL_MAX_COUNT = 10_000;
    static int lastUploadCount = 0;
    static int MAX_VISITS = 5;
    static final Set<String> emails = Collections.synchronizedSet(new HashSet<>(10_000));
    static final Set<String> emailsSentToDB = Collections.synchronizedSet(new HashSet<>(10_000));
    static Set<String> linksToVisit = Collections.synchronizedSet(new HashSet<>(20_000));
    static Set<String> linksFilter = Collections.synchronizedSet(new HashSet<>(20_000));
    static Set<String> linksVisited = Collections.synchronizedSet(new HashSet<>(10_000));
    static Map<String, Set<String>> maxLinksVisited = Collections.synchronizedMap(new HashMap<>());

    public static void main(String[] args) {

        ExecutorService pool = Executors.newFixedThreadPool(20);
        linksToVisit.add("https://www.touro.edu/");//start with touro.edu

        while (!linksToVisit.isEmpty() && emails.size() <= EMAIL_MAX_COUNT) {
            String link;
            synchronized (linksToVisit) {
                link = linksToVisit.stream().findFirst().get();
                linksToVisit.remove(link);
            }
            if (hasTooManyVisits(link)) {
                link = "";
            }
            if (!(link.equals(""))) {
                linksVisited.add(link);
                pool.execute(new WebScraper(link));
            }

            if (batchReadyForUpload() || lastUploadCount >= EMAIL_MAX_COUNT) {//upload in batches
                ArrayList<String> upload = new ArrayList<>(emails);
                upload.removeAll(emailsSentToDB);
                upload.forEach(Main::dbUpload);
                emailsSentToDB.addAll(upload);
                lastUploadCount += 100;
            }
        }
        pool.shutdownNow();
    }

    private static boolean hasTooManyVisits(String link) {
        try {
            URL currentURL = new URL(link);
            String host = currentURL.getHost();

            int startIndex = 0;
            int nextIndex = host.indexOf('.');
            int lastIndex = host.lastIndexOf('.');
            while (nextIndex < lastIndex) {
                startIndex = nextIndex + 1;
                nextIndex = host.indexOf('.', startIndex);
            }
            synchronized (maxLinksVisited) {
                if (startIndex > 0) {
                    Set<String> tempSet = maxLinksVisited.get(host.substring(startIndex));
                    if (tempSet == null) {
                        tempSet = new HashSet<>();
                        maxLinksVisited.put(host.substring(startIndex), tempSet);
                    }
                    tempSet.add(link);
                    maxLinksVisited.put(host.substring(startIndex), tempSet);
                    if (maxLinksVisited.get(host.substring(startIndex)).size() >= MAX_VISITS) {
                        return true;
                    }
                } else {
                    Set<String> tempSet = maxLinksVisited.get(host);
                    if (tempSet == null) {
                        tempSet = new HashSet<>();
                        maxLinksVisited.put(host, tempSet);
                    }
                    tempSet.add(link);
                    maxLinksVisited.put(host, tempSet);
                    if (maxLinksVisited.get(host).size() >= MAX_VISITS) {
                        return true;
                    }
                }
            }
        } catch (MalformedURLException e) {
            return false;
        }
        return false;
    }

    private static boolean batchReadyForUpload() {
        final int BATCH_SIZE = 100;
        return emails.size() - lastUploadCount > BATCH_SIZE;
    }

    public static void dbUpload(String email) {
        String url = "database-1.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com:1433"; // should pull from AWS Secrets Manager, environment variable, Properties class (key, value pairs)
        String connectionUrl =
                String.format("jdbc:sqlserver://%s;databaseName=jacobi;user=admin;password=mco368Touro", url);

        try (Connection con = DriverManager.getConnection(connectionUrl); // Autoclosable
             Statement stmt = con.createStatement()) {

            String insertQuery = String.format("INSERT INTO Emails VALUES ('%s')", email);
            stmt.executeUpdate(insertQuery);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}