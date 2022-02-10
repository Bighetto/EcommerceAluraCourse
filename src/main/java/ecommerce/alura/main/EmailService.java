package ecommerce.alura.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class EmailService {

    public static void main(String[] args)
    {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL")); //definir qual topico o consumidor vai escutar
        while (true)
        {
            var records = consumer.poll(Duration.ofMillis(100)); //verificar se ha mensagens/registros dentro do topico a cada 100 milisegundos
            if (!records.isEmpty())
            {
                System.out.println("Encontrados " + records.count() + " registros");

                for (var record : records)
                {
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
        }

    }

    private static Properties properties(){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());//definir o id do grupo que esta consumindo
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, EmailService.class.getSimpleName() + "-" + UUID.randomUUID().toString());// definir nome da maquina que esta executando o consumer
        return properties;
    }
}
