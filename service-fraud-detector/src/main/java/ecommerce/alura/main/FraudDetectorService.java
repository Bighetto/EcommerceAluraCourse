package ecommerce.alura.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Properties;

public class FraudDetectorService {

    public static void main(String[] args)
    {

        var fraudService = new FraudDetectorService();
       try(var service = new KafkaService<Order>(FraudDetectorService.class.getSimpleName(),
               "ECOMMERCE_NEW_ORDER",
               fraudService::parse, Order.class, Map.of()))
       {
           service.run();
       }
    }

    void parse(ConsumerRecord<String, Order> record) {
        System.out.println("------------------------------------------");
        System.out.println("Processando novo registro, verificando fraude");
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
        System.out.println("Registro processado! ");
    }

    private static Properties properties(){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());//definir o id do grupo que esta consumindo
        return properties;
    }
}
