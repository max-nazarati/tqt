package mn.tqt.presentation;

import mn.tqt.presentation.dummy.DummyObject;
import mn.tqt.presentation.dummy.Entry;
import mn.tqt.presentation.dummy.NestedObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class Controller {

    @PostMapping
    public String readFromKafka(@RequestBody String query) {
        return "done!";
    }

    @GetMapping("/generate-dummy-data")
    public void setupDummyTopic() throws InterruptedException {
        var props = new java.util.Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonSerializer");
        props.put("schema.registry.url", "localhost:8081");

        var producer = new KafkaProducer<Integer, DummyObject>(props);

        var nestedObject = new NestedObject(2, List.of(new Entry(1, Map.of(1, "a", 2, "b")),
                new Entry(2, Map.of(3, "c", 4, "d", 5, "e"))));
        for (var i = 0; i < 100; i++) {
            var data = new DummyObject(i, nestedObject);
            var record = new ProducerRecord<>("my-topic", data.id(), data);
            Thread.sleep(2);
            producer.send(record);
        }

    }

}
