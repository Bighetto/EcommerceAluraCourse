package ecommerce.alura.main;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {



       try (var dispatcher = new KafkaDispatcher()){
           for (var i = 0; i < 10; i++){

               var key = UUID.randomUUID().toString();

               var value = key + "331312, 5441276453";
               dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

               var email = "Obrigado pelo seu pedido! Ja iniciamos o processamento! ";
               dispatcher.send("ECOMMERCE_SEND_EMAIL",key,email);
           }
       }
    }


}
