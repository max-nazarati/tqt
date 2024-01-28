package mn.tqt.dummy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Properties;

@RestController
public class DummyController {

    @GetMapping("/generate-dummy-data")
    public void setupDummyTopic() throws InterruptedException {
        var props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonSerializer");
        props.put("schema.registry.url", "localhost:9091");

        var producer = new KafkaProducer<Integer, DummyObject>(props);

        var nestedObject = new mn.tqt.dummy.NestedObject(2, List.of(
                new mn.tqt.dummy.Entry(1, Map.of(1, "a", 2, "b")),
                new mn.tqt.dummy.Entry(2, Map.of(3, "c", 4, "d", 5, "e"))));
        for (var i = 0; i < 100; i++) {
            var data = new mn.tqt.dummy.DummyObject(i, nestedObject);
            var record = new ProducerRecord<>("my-topic", data.id(), data);
            Thread.sleep(2);
            producer.send(record);
        }

        producer.close();

    }
}
