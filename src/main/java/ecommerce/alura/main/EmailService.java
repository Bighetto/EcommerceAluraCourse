package ecommerce.alura.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.UUID;

public class EmailService {

    public static void main(String[] args)
    {
        var emailService = new EmailService();
        try(var service = new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", emailService::parse))
        {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String,String> record){
        System.out.println("------------------------------------------");
        System.out.println("Enviando email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try
        {
            Thread.sleep(5000); // esperar 5 segundos pra cada registro
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        System.out.println("Email Enviado! ");
    }
}
