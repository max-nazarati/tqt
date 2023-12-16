package mn.tqt.local_kafka_initializer

fun main() {
    val controller = DummyController()

    controller.populateKafka()
}
