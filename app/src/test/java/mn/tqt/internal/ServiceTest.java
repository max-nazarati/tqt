package mn.tqt.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import mn.tqt.Query;
import mn.tqt.QuerySchema;
import mn.tqt.SchemaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServiceTest {

    ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    KafkaReader kafkaReader;

    @InjectMocks
    Service underTest;

    @Test
    void excludeNestedFields() throws Exception {
        ObjectNode record = (ObjectNode) objectMapper.readTree("{\"a\": {\"b\": 1}, \"c\": 2}");
        List<ObjectNode> expectedObjects = List.of((ObjectNode) objectMapper.readTree("{\"c\": 2}"));

        Query query = simpleQuery(new QuerySchema(SchemaType.EXCLUDE, List.of("a.b")));
        var kafkaObjects = new KafkaObjects(query.schema());
        kafkaObjects.add(record);

        when(kafkaReader.executeQuery(eq(query), any())).thenReturn(kafkaObjects);

        // ASSERTION
        assertThat(expectedObjects).isEqualTo(underTest.readJsonRecordsWithSchema(query));
    }

    @Test
    void includeNestedFields() throws Exception {
        ObjectNode record = (ObjectNode) objectMapper.readTree("{\"a\": {\"b\": 1}, \"c\": {\"d\": 2}}");
        List<ObjectNode> expectedJson = List.of((ObjectNode) objectMapper.readTree("{\"a\":{\"b\":1}}"));

        Query query = simpleQuery(new QuerySchema(SchemaType.INCLUDE, List.of("a.b")));
        var kafkaObjects = new KafkaObjects(query.schema());
        kafkaObjects.add(record);

        when(kafkaReader.executeQuery(eq(query), any())).thenReturn(kafkaObjects);

        // ASSERTION
        assertThat(expectedJson).isEqualTo(underTest.readJsonRecordsWithSchema(query));
    }

    @Test
    void listOfObjects() throws Exception {
        ObjectNode record = (ObjectNode) objectMapper.readTree("{\"1\": [{\"b\": 1, \"c\": 2}]}");
        List<ObjectNode> expectedJson = List.of((ObjectNode) objectMapper.readTree("{\"1\": [{\"b\": 1}]}"));

        Query query = simpleQuery(new QuerySchema(SchemaType.EXCLUDE, List.of("1.c")));
        var kafkaObjects = new KafkaObjects(query.schema());
        kafkaObjects.add(record);

        when(kafkaReader.executeQuery(eq(query), any())).thenReturn(kafkaObjects);

        // ASSERTION
        assertThat(expectedJson).isEqualTo(underTest.readJsonRecordsWithSchema(query));
    }

    private Query simpleQuery(QuerySchema schema) {
        return new Query("", "",  "", schema);
    }


}