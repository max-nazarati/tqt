package mn.tqt.internal;

import com.fasterxml.jackson.databind.node.ObjectNode;
import mn.tqt.ConsumerFactory;
import mn.tqt.EnvPropertyReader;
import mn.tqt.Query;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;

@org.springframework.stereotype.Service
public class Service {
    private final KafkaReader kafkaReader;
    private final EnvPropertyReader envPropertyReader = new EnvPropertyReader();

    Service(KafkaReader kafkaReader) {
        this.kafkaReader = kafkaReader;
    }

    public List<ObjectNode> readJsonRecordsWithSchema(Query query) {
         String kafkaUrl = envPropertyReader.kafkaServerUrl();
         String schemaRegistryUrl = envPropertyReader.schemaRegistryUrl();
        KafkaConsumer<?, ObjectNode> consumer = ConsumerFactory.buildJsonSchemaConsumer(kafkaUrl, schemaRegistryUrl);
        return kafkaReader.executeQuery(query, consumer).toList();
    }

    public List<ObjectNode> readJsonRecords(Query query) {
        String kafkaUrl = envPropertyReader.kafkaServerUrl();
        String schemaRegistryUrl = envPropertyReader.schemaRegistryUrl();

        KafkaConsumer<?, ObjectNode> consumer = ConsumerFactory.buildSimpleJsonConsumer(kafkaUrl, schemaRegistryUrl);
        return kafkaReader.executeQuery(query, consumer).toList();
    }

    public List<ObjectNode> readAvroRecordsWithSchema(Query query) {
        String kafkaUrl = envPropertyReader.kafkaServerUrl();
        String schemaRegistryUrl = envPropertyReader.schemaRegistryUrl();

        KafkaConsumer<?, GenericRecord> consumer = ConsumerFactory.buildAvroSchemaConsumer(kafkaUrl, schemaRegistryUrl);
        return kafkaReader.executeQuery(query, consumer).toList();

    }


}
