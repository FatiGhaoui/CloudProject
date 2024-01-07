package fr.emse.worker.SQS;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class SendMessage {

    private final SqsClient sqsClient;

    public SendMessage() {
        this.sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    public SendMessageResponse sendSqsMessage(String queueUrl, String messageBody) {
        SendMessageRequest sendRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        return sqsClient.sendMessage(sendRequest);
    }
}
