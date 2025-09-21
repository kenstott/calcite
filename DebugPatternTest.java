import java.nio.file.*;
import java.util.*;

public class DebugPatternTest {
    public static void main(String[] args) throws Exception {
        Path basePath = Paths.get("/Volumes/T9/govdata-parquet/source=econ");
        String pattern = "type=indicators/year=*/fred_indicators.parquet";

        System.out.println("Testing pattern: " + pattern);
        System.out.println("Base path: " + basePath);

        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);

        Files.walk(basePath)
            .filter(Files::isRegularFile)
            .forEach(file -> {
                Path relativePath = basePath.relativize(file);
                boolean matches = matcher.matches(relativePath);
                if (file.getFileName().toString().equals("fred_indicators.parquet")) {
                    System.out.println("File: " + relativePath + " -> matches: " + matches);
                }
            });
    }
}