import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class Consumidor implements AutoCloseable{

    private Connection connection;
    private Channel channel;
    private String QUEUE_NAME = "rpc_fila";

    public Consumidor() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("mqadmin");
        factory.setPassword("Admin123XX_");

        connection = factory.newConnection();
        channel = connection.createChannel();

    }

    public static void main(String[] args) throws Exception {
        try (Consumidor client = new Consumidor()) {
            for (int i = 0; i < 5; i++) {
                String response = client.call("Carlos Manoel Nunes e Silva");
                System.out.println(response);
            }
        } catch (IOException | TimeoutException | InterruptedException err) {
            err.printStackTrace();
        }
    }

    public String call(String message) throws IOException, InterruptedException {
        String corrId = UUID.randomUUID().toString();

        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties()
                .builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", QUEUE_NAME, props, message.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        DeliverCallback callback = (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        };

        String ctag = channel.basicConsume(replyQueueName, true, callback, consumerTag -> {});

        String result = response.take();
        channel.basicCancel(ctag);
        return result;
    }

    @Override
    public void close() throws Exception {
        connection.close();

    }
}
