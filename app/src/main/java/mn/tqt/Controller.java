package mn.tqt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import mn.tqt.internal.KafkaReader;
import mn.tqt.internal.NodeManipulation;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class Controller {

    private final ConsumerFactory<Integer> consumerFactory;
    private final KafkaReader<Integer, JsonNode> kafkaReader;

    public Controller(ConsumerFactory<Integer> consumerFactory, KafkaReader<Integer, JsonNode> kafkaReader) {
        this.consumerFactory = consumerFactory;
        this.kafkaReader = kafkaReader;
    }

    @PostMapping
    public List<JsonNode> readFromKafka(@RequestBody Query query) {
        var consumer = consumerFactory.buildConsumer(query.kafkaEndpoint(), query.schemaRegistry());

        return kafkaReader.readRecords(query, consumer)
                .stream().map(r -> NodeManipulation.applySchema(r, query)).toList();
    }

}
