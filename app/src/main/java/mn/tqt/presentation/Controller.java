package mn.tqt.presentation;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import mn.tqt.presentation.dummy.KafkaReader;
import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

@RestController
public class Controller {

    @PostMapping
    public java.util.List<ObjectNode> readFromKafka(@RequestBody Query query) throws InterruptedException {
        var consumer = buildConsumer(query.kafkaEndpoint(), query.kafkaTopic(), query.schemaRegistry());

        var reader = new KafkaReader<>(
                consumer,
                query.kafkaTopic(),
                query.typedStartDate().toInstant().toEpochMilli(),
                query.typedEndDate().toInstant().toEpochMilli());

        ArrayList<ConsumerRecord<Integer, JsonNode>> consumerRecords = reader.readRecords();
        return consumerRecords.stream().map(record -> (ObjectNode) record.value())
                .map(json -> applySchema(json, query)).toList();
    }

    private ObjectNode applySchema(ObjectNode json, Query query) {

        var pointers = new ArrayList<JsonPointer>();
        switch (query.schema().type()) {
            case EXCLUDE -> pointers.addAll(constructSchemaPointers(json.deepCopy(), query.schemaAsListOfQueues()));
            case INCLUDE -> {
                List<JsonPointer> schemaPointers =
                        constructSchemaPointers(json.deepCopy(), query.schemaAsListOfQueues());
                List<JsonPointer> allPointers = constructAllJsonPointers(json,
                        JsonPointer.compile("/"));

                List<JsonPointer> deletionPointers = allPointers.stream()
                        .filter(pointer -> schemaPointers.stream()
                                .noneMatch(schemaPointer -> {
                                    if (pointer.length() <= schemaPointer.length()) {
                                        return schemaPointer.toString().startsWith(pointer.toString());
                                    } else {
                                        return pointer.toString().startsWith(schemaPointer.toString());
                                    }
                                }))
                        .sorted((l, r) -> Integer.compare(pointerDepth(r), pointerDepth(l)))
                        .toList();

                pointers.addAll(deletionPointers);
            }
        }

        var jsonCopy = json.deepCopy();
        for (var pointer : pointers) {
            var currentObject = (ObjectNode) jsonCopy.at(pointer.head());
            currentObject.remove(pointer.last().getMatchingProperty());
        }

        return jsonCopy;
    }

    private int pointerDepth(JsonPointer pointer) {
        return pointer.toString().split("/").length;
    }

    private List<JsonPointer> constructAllJsonPointers(JsonNode json, JsonPointer pointer) {
        var jsonCopy = json.deepCopy();
        var acc = new ArrayList<JsonPointer>();

        if (jsonCopy.isValueNode()) {
            return List.of(pointer);
        } else if (jsonCopy.isObject()) {
            for (var property : jsonCopy.properties()) {

                acc.addAll(constructAllJsonPointers(property.getValue(), pointer.appendProperty(property.getKey())));
            }
        } else if (jsonCopy.isArray()) {
            for (int i = 0; i < jsonCopy.size(); i++) {
                acc.addAll(constructAllJsonPointers(jsonCopy.get(i), pointer.appendIndex(i)));
            }
        }

        return acc;
    }

    private List<JsonPointer> constructSchemaPointers(ObjectNode json, List<LinkedList<String>> paths) {
        var acc = new ArrayList<JsonPointer>();
        for (var path : paths) {
            acc.addAll(constructSchemaPointersForPath(json, JsonPointer.compile("/"), path));
        }
        return acc;
    }

    private List<JsonPointer> constructSchemaPointersForPath(
            JsonNode json,
            JsonPointer pointer,
            LinkedList<String> path) {
        var acc = new ArrayList<JsonPointer>();
        while (!path.isEmpty()) {
            var subPath = path.pop();

            json = json.get(subPath);
            if (path.isEmpty()) {
                pointer = pointer.appendProperty(subPath);
                acc.add(pointer);
            } else if (json.isObject()) {
                pointer = pointer.appendProperty(subPath);
            } else if (json.isArray()) {

                var arrayPointer = pointer.appendProperty(subPath);
                for (int i = 0; i < json.size(); i++) {
                    var arrayPointers = constructSchemaPointersForPath(json.deepCopy(),
                            arrayPointer.appendIndex(i),
                            (LinkedList<String>) path.clone());
                    acc.addAll(arrayPointers);
                }

                path.clear();
            }
        }

        return acc;
    }

    private KafkaConsumer<Integer, JsonNode> buildConsumer(
            String server,
            String topic,
            String schemaRegistry) {
        var props = new java.util.Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("json.value.type", "com.fasterxml.jackson.databind.JsonNode");
        props.put("schema.registry.url", schemaRegistry);

        var consumer = new KafkaConsumer<Integer, JsonNode>(props);
        consumer.subscribe(java.util.List.of(topic));

        return consumer;
    }

    @GetMapping("/generate-dummy-data")
//    @Async
    public void setupDummyTopic() throws InterruptedException {
        var props = new java.util.Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonSerializer");
        props.put("schema.registry.url", "localhost:9091");

        var producer = new KafkaProducer<Integer, mn.tqt.presentation.dummy.DummyObject>(props);

        var nestedObject = new mn.tqt.presentation.dummy.NestedObject(2, java.util.List.of(
                new mn.tqt.presentation.dummy.Entry(1, java.util.Map.of(1, "a", 2, "b")),
                new mn.tqt.presentation.dummy.Entry(2, java.util.Map.of(3, "c", 4, "d", 5, "e"))));
        for (var i = 0; i < 100; i++) {
            var data = new mn.tqt.presentation.dummy.DummyObject(i, nestedObject);
            var record = new ProducerRecord<>("my-topic", data.id(), data);
            Thread.sleep(2);
            producer.send(record);
        }

        producer.close();

    }

}
