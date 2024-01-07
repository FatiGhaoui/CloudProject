package fr.emse.worker.SQS;

public class Test {
    public static void main(String[] args) {
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/240971291223/messaging-app-queue";
        ReadMessage reader = new ReadMessage(queueUrl);

        for (int i = 0; i < 10; i++) { // Répéter plusieurs fois pour récupérer plus de messages
            var messages = reader.receiveMessages();

            if (messages != null && !messages.isEmpty()) {
                for (var message : messages) {
                    System.out.println("Received message: " + message.body());

                }
            } else {
                System.out.println("No more messages found in the queue.");
                break;
            }
        }
    }

}
