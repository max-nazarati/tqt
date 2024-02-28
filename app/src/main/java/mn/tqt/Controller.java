package mn.tqt;

import com.fasterxml.jackson.databind.node.ObjectNode;
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

    @PostMapping("/schema/json")
    public List<ObjectNode> readJsonFromKafka(@RequestBody Query query) {
            return service.readJsonRecordsWithSchema(query);
    }

    @PostMapping("/json")
    public List<ObjectNode> readSimpleJsonFromKafka(@RequestBody Query query) {
            return service.readJsonRecords(query);
    }

    @PostMapping("/schema/avro")
    public List<ObjectNode> readAvroFromKafka(@RequestBody Query query) {
            return service.readAvroRecordsWithSchema(query);
    }

}
