package ecommerce.alura.main;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try (var emailDispatcher = new KafkaDispatcher<Email>())
        {
            try (var orderDispatcher = new KafkaDispatcher<Order>())
            {
                for (var i = 0; i < 10; i++)
                {

                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random()  * 5000 + 1);
                    var order = new Order(userId, orderId, amount);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);


                    var email = new Email("Pedido em processamento!", "Obrigado pelo pedido! Ja estamos a processa-lo");
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId ,email);
                }
            }
        }
    }


}
