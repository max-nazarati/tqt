package mn.tqt.dummy;

import java.util.Map;

public record Entry(int id, Map<Integer, String> someMap) {
}
