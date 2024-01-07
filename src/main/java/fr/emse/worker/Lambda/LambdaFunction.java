package fr.emse.worker.Lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class LambdaFunction implements RequestHandler<S3Event, String> {
    private static final DateTimeFormatter FILE_DATE_FORMAT = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private static final DateTimeFormatter FILE_NAME_DATE_FORMAT = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private final S3Client s3Client = S3Client.builder().build();

    @Override
    public String handleRequest(S3Event event, Context context) {
        event.getRecords().forEach(record -> {
            String bucketName = record.getS3().getBucket().getName();
            String fileKey = record.getS3().getObject().getKey();

            String dateString = fileKey.substring(0, 10); // Assuming the date is at the start of the file name.
            LocalDate processDate = LocalDate.parse(dateString, FILE_DATE_FORMAT);
            processFiles(bucketName, processDate, context);
        });

        return "Lambda invocation complete.";
    }

    private void processFiles(String bucketName, LocalDate processDate, Context context) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(bucketName).build();
        ListObjectsV2Iterable listResponse = s3Client.listObjectsV2Paginator(listRequest);

        Map<String, Double> totalProfitByStore = new HashMap<>();
        Map<String, Double> totalProfitByProduct = new HashMap<>();
        Map<String, Integer> totalQuantityByProduct = new HashMap<>();
        Map<String, Double> totalSoldByProduct = new HashMap<>();

        for (S3Object s3Object : listResponse.contents()) {
            if (s3Object.key().contains(processDate.format(FILE_DATE_FORMAT))) {
                context.getLogger().log("Processing file: " + s3Object.key());
                processFile(bucketName, s3Object.key(), totalProfitByStore, totalProfitByProduct, totalQuantityByProduct, totalSoldByProduct, context);
            }
        }

        saveToCsv(totalProfitByStore, totalProfitByProduct, totalQuantityByProduct, totalSoldByProduct, context);
    }

    private void processFile(String bucketName, String key, Map<String, Double> totalProfitByStore, Map<String, Double> totalProfitByProduct, Map<String, Integer> totalQuantityByProduct, Map<String, Double> totalSoldByProduct, Context context) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
        try (ResponseInputStream<GetObjectResponse> s3is = s3Client.getObject(getObjectRequest);
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3is))) {

            String line;
            reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(";");
                if (parts.length != 8) {
                    context.getLogger().log("Unexpected format at line: " + line);
                    continue;
                }

                String store = parts[1].trim();
                String product = parts[2].trim();
                int quantity = Integer.parseInt(parts[3].trim());
                double unitPrice = Double.parseDouble(parts[4].trim());
                double totalProfit = Double.parseDouble(parts[6].trim()) * quantity;
                double totalSold = unitPrice * quantity;

                totalProfitByStore.merge(store, totalProfit, Double::sum);
                totalProfitByProduct.merge(product, totalProfit, Double::sum);
                totalQuantityByProduct.merge(product, quantity, Integer::sum);
                totalSoldByProduct.merge(product, totalSold, Double::sum);
            }
        } catch (IOException e) {
            context.getLogger().log("Error processing file: " + key + " - " + e.getMessage());
        }
    }

    private void saveToCsv(Map<String, Double> totalProfitByStore, Map<String, Double> totalProfitByProduct, Map<String, Integer> totalQuantityByProduct, Map<String, Double> totalSoldByProduct, Context context) {
        LocalDate today = LocalDate.now();
        String fileName = "summary-" + today.format(FILE_NAME_DATE_FORMAT) + ".csv";

        try {
            Path tempFile = Files.createTempFile("summary", ".csv");
            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(tempFile))) {
                // Store-related data
                writer.println("By Store");
                writer.println("Store Name;Total Profit");
                totalProfitByStore.forEach((store, profit) -> writer.println(store + ";" + profit));

                writer.println(); // Blank line for separation

                // Product-related data
                writer.println("By Product");
                writer.println("Product Name;Total Profit;Total Quantity;Total Sold");
                totalProfitByProduct.keySet().forEach(product -> {
                    Double profit = totalProfitByProduct.getOrDefault(product, 0.0);
                    Integer quantity = totalQuantityByProduct.getOrDefault(product, 0);
                    Double sold = totalSoldByProduct.getOrDefault(product, 0.0);
                    writer.println(product + ";" + profit + ";" + quantity + ";" + sold);
                });
            }

            // Upload to S3 in the specified bucket
            s3Client.putObject(PutObjectRequest.builder().bucket("mybucket1308").key(fileName).build(), tempFile);
            Files.delete(tempFile);
        } catch (IOException e) {
            context.getLogger().log("Error saving summary file: " + e.getMessage());
        }
    }
}
