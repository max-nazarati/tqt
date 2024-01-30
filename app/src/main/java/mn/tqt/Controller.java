package mn.tqt;

import com.fasterxml.jackson.databind.node.ObjectNode;
import mn.tqt.internal.KafkaReader;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class Controller {

    private final ConsumerFactory consumerFactory;

    public Controller(ConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @PostMapping
    public List<ObjectNode> readFromKafka(@RequestBody Query query) {
        var consumer = consumerFactory.buildConsumer(query.kafkaEndpoint(), query.schemaRegistry());

        var reader = new KafkaReader<>(
                consumer,
                query.kafkaTopic(),
                query.typedStartDate().toInstant().toEpochMilli(),
                query.typedEndDate().toInstant().toEpochMilli());

        return reader.readRecords(query);
    }

}
