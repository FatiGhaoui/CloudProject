package fr.emse.worker.SQS;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

public class ReadMessage {
    private static final Logger LOGGER = Logger.getLogger(ReadMessage.class.getName());
    private final SqsClient sqsClient;
    private final String queueUrl;

    public ReadMessage(String queueUrl) {
        this.sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        this.queueUrl = queueUrl;
    }

    public List<Message> receiveMessages() {
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .build();

            return sqsClient.receiveMessage(receiveRequest).messages();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to receive messages from SQS", e);
            return null;
        }
    }
}
