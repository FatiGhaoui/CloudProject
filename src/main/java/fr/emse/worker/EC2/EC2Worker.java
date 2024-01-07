package fr.emse.worker.EC2;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import fr.emse.worker.SQS.ReadMessage;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EC2Worker {

    private static final String BUCKET_NAME = "clientbucket13"; // Replace with your actual bucket name
    private static final String SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/240971291223/messaging-app-queue"; // Replace with your actual queue URL
    private static final DateTimeFormatter FILE_DATE_FORMAT = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private final S3Client s3Client = S3Client.builder().build();
    private final SqsClient sqsClient = SqsClient.builder().build();

    public static void main(String[] args) {
        EC2Worker app = new EC2Worker();
        ReadMessage sqsReader = new ReadMessage(SQS_QUEUE_URL);

        System.out.println("Starting to poll for messages...");

        while (true) {
            List<Message> messages = sqsReader.receiveMessages();
            if (messages.isEmpty()) {
                System.out.println("No new messages in the queue.");
            } else {
                System.out.println("Received " + messages.size() + " messages. Processing...");
            }

            for (Message message : messages) {
                try {
                    String messageBody = message.body();
                    System.out.println("Processing message: " + messageBody);

                    int startIndex = messageBody.indexOf(':') + 2;
                    int endIndex = messageBody.lastIndexOf('-');
                    if (startIndex < 0 || endIndex < 0 || endIndex <= startIndex) {
                        System.out.println("Invalid message format: " + messageBody);
                        continue; // Skip this message
                    }


                    LocalDate processDate;
                    try {
                        String dateString = messageBody.substring(startIndex, endIndex).trim();
                        processDate = LocalDate.parse(dateString, FILE_DATE_FORMAT);
                        System.out.println("Processing date: " + processDate);
                    } catch (DateTimeParseException e) {
                        System.out.println("Error parsing date from message, using today's date: " + message.body());
                        e.printStackTrace();
                        continue;
                    }


                    app.processFiles(BUCKET_NAME, processDate);

                    app.deleteMessageFromQueue(message);
                    System.out.println("Message processed and deleted from the queue.");
                } catch (DateTimeParseException e) {
                    System.out.println("Error parsing date from message: " + message.body());
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private void deleteMessageFromQueue(Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(SQS_QUEUE_URL)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteRequest);
    }

    private void processFiles(String bucketName, LocalDate processDate) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(bucketName).build();
        ListObjectsV2Iterable listResponse = s3Client.listObjectsV2Paginator(listRequest);

        Map<String, Double> totalProfitByStore = new HashMap<>();
        Map<String, Double> totalProfitByProduct = new HashMap<>();
        Map<String, Integer> totalQuantityByProduct = new HashMap<>();
        Map<String, Double> totalSoldByProduct = new HashMap<>();

        for (S3Object s3Object : listResponse.contents()) {
            if (s3Object.key().contains(processDate.format(FILE_DATE_FORMAT))) {
                System.out.println("Processing file: " + s3Object.key());
                processFile(bucketName, s3Object.key(), totalProfitByStore, totalProfitByProduct, totalQuantityByProduct, totalSoldByProduct);
            }
        }

        saveToCsv("mybucket1308", "summary-" + processDate.format(FILE_DATE_FORMAT) + ".csv", totalProfitByStore, totalProfitByProduct, totalQuantityByProduct, totalSoldByProduct);
    }

    private void processFile(String bucketName, String key, Map<String, Double> totalProfitByStore, Map<String, Double> totalProfitByProduct, Map<String, Integer> totalQuantityByProduct, Map<String, Double> totalSoldByProduct) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
        try (ResponseInputStream<GetObjectResponse> s3is = s3Client.getObject(getObjectRequest);
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3is))) {

            String line;
            reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",|;");

                if (parts.length != 8) {
                    System.out.println("Unexpected format at line: " + line);
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
            System.out.println("Error processing file: " + key + " - " + e.getMessage());
        }
    }


    private void saveToCsv(String bucketName, String fileName, Map<String, Double> totalProfitByStore, Map<String, Double> totalProfitByProduct, Map<String, Integer> totalQuantityByProduct, Map<String, Double> totalSoldByProduct) {
        Path tempFile = null;
        try {
            tempFile = Files.createTempFile(fileName.replace(".csv", ""), ".csv");
            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(tempFile))) {
                writer.println("By Store");
                writer.println("Store Name;Total Profit");
                totalProfitByStore.forEach((store, profit) -> writer.println(store + ";" + profit));
                writer.println();

                writer.println("By Product");
                writer.println("Product Name;Total Profit;Total Quantity;Total Sold");
                totalProfitByProduct.forEach((product, profit) -> {
                    Double sold = totalSoldByProduct.getOrDefault(product, 0.0);
                    Integer quantity = totalQuantityByProduct.getOrDefault(product, 0);
                    writer.println(product + ";" + profit + ";" + quantity + ";" + sold);
                });
            }

            // Display file content before uploading
            System.out.println("Content of the file before uploading:");
            Files.readAllLines(tempFile).forEach(System.out::println);

            // Proceed with the upload
            s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(fileName).build(), tempFile);
        } catch (IOException e) {
            System.out.println("Error saving summary file: " + e.getMessage());
        } finally {
            if (tempFile != null) {
                try {
                    Files.delete(tempFile);
                } catch (IOException e) {
                    System.out.println("Error deleting the temporary file: " + e.getMessage());
                }
            }
        }
    }

}
