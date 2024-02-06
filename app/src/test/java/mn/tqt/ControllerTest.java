package mn.tqt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import mn.tqt.internal.KafkaReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ControllerTest {

    @Mock
    ConsumerFactory<Integer> consumerFactory;

    @Mock
    KafkaConsumer<Integer, JsonNode> consumer;

    @Mock
    KafkaReader<Integer, JsonNode> kafkaReader;

    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    Controller underTest;

    @BeforeEach
    void setup() {
        when(consumerFactory.buildConsumer(any(), any())).thenReturn(consumer);
    }

    @Test
    void nestedFields() throws JsonProcessingException {
        JsonNode simpleJson = objectMapper.readTree("{\"a\": {\"b\": 1}, \"c\": 2}");
        JsonNode expectedJson = objectMapper.readTree("{\"a\": {}, \"c\": 2}");

        Query query = simpleQuery(new QuerySchema(SchemaType.EXCLUDE, List.of("a.b")));

        when(kafkaReader.readRecords(query, consumer)).thenReturn(List.of(new ConsumerRecord<>("dummyTopic", 0, 0, 1, simpleJson)));

        // ASSERTION
        var result = underTest.readFromKafka(query);
        assertEquals(result, List.of(expectedJson));
    }

    @Test
    void listOfObjects() throws JsonProcessingException {
        JsonNode simpleJson = objectMapper.readTree("{\"1\": [{\"b\": 1, \"c\": 2}]}");
        JsonNode expectedJson = objectMapper.readTree("{\"1\": [{\"b\": 1}]}");

        Query query = simpleQuery(new QuerySchema(SchemaType.EXCLUDE, List.of("1.c")));

        when(kafkaReader.readRecords(query, consumer)).thenReturn(List.of(new ConsumerRecord<>("dummyTopic", 0, 0, 1, simpleJson)));

        // ASSERTION
        var result = underTest.readFromKafka(query);
        assertEquals(result, List.of(expectedJson));
    }

    private Query simpleQuery(QuerySchema schema) {
        return new Query("", "", "", "", "", schema);
    }

}