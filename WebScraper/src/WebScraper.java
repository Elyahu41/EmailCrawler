import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebScraper implements Runnable {

    String currentUrl;
    String[] randomFileExtensions = {"png", "jpg", "gif", "pdf", "mp3", "css", "mp4", "mov", "7z", "zip", "mkv", "avi", "jpeg"};//common files

    WebScraper(String url) {
        this.currentUrl = url;
        run();
    }

    @Override
    public void run() {
        scrape(currentUrl);
    }

    public void scrape(String url) {
        try {
            try { // double try block so the program doesn't stop on errors
                Document doc = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) Gecko/20070725 Firefox/2.0.0.6")
                        .referrer("http://www.google.com").get();

                Pattern emailPattern = Pattern.compile("[\\w\\d._]+@[\\w\\d]+\\.[\\w]{2,3}");
                Matcher emailMatcher = emailPattern.matcher(doc.toString());

                while (emailMatcher.find()) {//find and add emails
                    String email = emailMatcher.group();
                    if (Arrays.stream(randomFileExtensions).parallel().noneMatch(email::contains)) {//filter for any files that are not emails
                        Main.emails.add(emailMatcher.group());
                    }
                }
                synchronized (Main.linksFilter) {
                    Main.linksFilter.addAll(doc.select("a[href]").eachAttr("abs:href"));//find and add all links on the page
                    for (String randomFileExtension : randomFileExtensions) {
                        Main.linksFilter.removeIf(s -> s.contains(randomFileExtension));//filter links for any files
                    }
                    synchronized (Main.linksToVisit) {
                        Main.linksFilter.removeAll(Main.linksVisited);
                        Main.linksToVisit.addAll(Main.linksFilter);
                        Main.linksFilter.clear();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}