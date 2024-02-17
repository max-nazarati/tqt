package mn.tqt;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;


@SpringBootTest
@Testcontainers
class ControllerIT {

    ObjectMapper objectMapper = new ObjectMapper();

    @Container
    static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("bitnami/kafka:3.2.3"));

    @Test
    void test() {}

    private Query simpleQuery(QuerySchema schema) {
        return new Query("", "", "", "", "", schema);
    }

}