package ecommerce.alura.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
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
        var order = record.value();

        if (isFraud(order)){
            //se o valor for maior que 4500 sera fraude
            System.out.println("Este pedido é uma fraude: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
        }else {
            System.out.println("Aprovado: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getUserId(), order);
        }


        System.out.println("Registro processado! ");
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

    private static Properties properties(){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());//definir o id do grupo que esta consumindo
        return properties;
    }
}
