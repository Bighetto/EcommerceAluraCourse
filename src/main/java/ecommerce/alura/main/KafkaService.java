package ecommerce.alura.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

class KafkaService implements Closeable {
    private final KafkaConsumer<String, String> consumer;
    final ConsumerFunction parse;

    KafkaService(String groupId, String topic, ConsumerFunction parse)
    {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(groupId));
        consumer.subscribe(Collections.singletonList(topic)); //definir qual topico o consumidor vai escutar

    }

    void run()
    {

        while (true)
        {
            var records = consumer.poll(Duration.ofMillis(100)); //verificar se ha mensagens/registros dentro do topico a cada 100 milisegundos
            if (!records.isEmpty())
            {
                System.out.println("Encontrados " + records.count() + " registros");
                for (var record : records)
                {
                    parse.consume(record);
                }
            }
        }
    }

    private static Properties properties(String groupId){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);//definir o id do grupo que esta consumindo
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());// definir nome da maquina que esta executando o consumer
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
