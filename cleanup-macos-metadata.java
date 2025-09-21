import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

public class CleanupMacOSMetadata {
    public static void main(String[] args) throws Exception {
        LocalFileStorageProvider provider = new LocalFileStorageProvider();
        String econDir = "/Volumes/T9/govdata-parquet/source=econ";

        System.out.println("Cleaning up macOS metadata in: " + econDir);
        provider.cleanupMacosMetadata(econDir);
        System.out.println("Cleanup completed.");
    }
}