package fr.emse.client;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Scanner;

public class CsvFileUploader {
    private static final Logger LOGGER = Logger.getLogger(CsvFileUploader.class.getName());
    private S3Client s3;
    private static final Region region = Region.US_EAST_1;
    private static final String bucket = "clientbucket13";


    private static final String DEFAULT_FILE_PATH = "~/Awsl/CsvFiles/";

    public CsvFileUploader() {
        this.s3 = S3Client.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    public void uploadFile(String bucketName, String fileKey, String filePath) {
        try {
            if (!fileKey.toLowerCase().endsWith(".csv")) {
                LOGGER.warning("Only CSV files are allowed.");
                return;
            }

            Path resolvedFilePath = resolveHomeDirectory(filePath);

            if (!Files.exists(resolvedFilePath)) {
                LOGGER.warning("File does not exist at path: " + resolvedFilePath);
                return;
            }

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileKey)
                    .build();

            s3.putObject(putObjectRequest, resolvedFilePath);
            LOGGER.info("File uploaded successfully to bucket " + bucketName + " as " + fileKey);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred while uploading file", e);
        }
    }

    private Path resolveHomeDirectory(String filePath) {
        if (filePath.startsWith("~" + System.getProperty("file.separator"))) {
            filePath = System.getProperty("user.home") + filePath.substring(1);
        }
        return Paths.get(filePath);
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the filename to upload: ");
        String fileName = scanner.nextLine();

        System.out.print("Enter the file path (leave blank for default): ");
        String inputPath = scanner.nextLine();
        String filePath = inputPath.isEmpty() ? DEFAULT_FILE_PATH + fileName : inputPath;

        CsvFileUploader uploader = new CsvFileUploader();
        uploader.uploadFile(bucket, fileName, filePath);

        scanner.close();
    }
}
