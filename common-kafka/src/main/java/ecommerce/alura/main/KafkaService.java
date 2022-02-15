package ecommerce.alura.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties)
    {
        this(parse, groupId, type, properties);
        consumer.subscribe(Collections.singletonList(topic)); //definir qual topico o consumidor vai escutar

    }

    KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(parse,groupId,type, properties);
        consumer.subscribe(topic); //definir qual topico o consumidor vai escutar
    }

    private KafkaService(ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(type, groupId, properties));
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
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        // apenas captura excecoes e segue para o proximo registro
                        // apenas logar o erro que esta acontecendo e tenta novamente
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overrideProperties){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GSonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);//definir o id do grupo que esta consumindo
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());// definir nome da maquina que esta executando o consumer
        properties.setProperty(GSonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
