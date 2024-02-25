package mn.tqt;

import com.fasterxml.jackson.databind.JsonNode;
import mn.tqt.internal.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class Controller {

    private final Service service;

    public Controller(Service service) {
        this.service = service;
    }

    @PostMapping("/json")
    public List<JsonNode> readJsonFromKafka(@RequestBody Query query) {
        return service.readJsonRecordsWithSchema(query);
    }

    @PostMapping("/avro")
    public List<JsonNode> readAvroFromKafka(@RequestBody Query query) {
        return service.readAvroRecordsWithSchema(query);
    }

}
