package kafkacluster.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Консольный consumer: читает сообщения из топика {@code msgtopic}
 * и печатает их в консоль.
 */
public class ConsumerApp {

    private static final String TOPIC = "msgtopic";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1_000L);

    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_PORT = "9092";

    public static void main(String[] args) {
        // Адрес брокера задаётся аргументами командной строки --host и --port,
        // что позволяет подключать consumer к разным кластерам Kafka.
        // По умолчанию — локальный кластер из kafkacluster-docker (localhost:9092).
        String host = getArg(args, "--host", DEFAULT_HOST);
        String port = getArg(args, "--port", DEFAULT_PORT);
        String bootstrapServers = host + ":" + port;
        // Группа потребителей; при желании переопределяется через GROUP_ID.
        String groupId = System.getenv().getOrDefault("GROUP_ID", "kafkacluster-consumer");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // При первом запуске (нет сохранённого offset) читаем топик с самого начала.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafkacluster-consumer");

        System.out.println("Consumer запущен. Брокер: " + bootstrapServers
                + ", топик: " + TOPIC + ", группа: " + groupId);
        System.out.println("Ожидание сообщений... Остановка — Ctrl+C.");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Корректное завершение по Ctrl+C: wakeup() прерывает блокирующий poll().
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nЗавершение работы consumer...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(List.of(TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Получено <- partition=%d offset=%d key=%s : %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (WakeupException e) {
            // Ожидаемое исключение при остановке — игнорируем.
        } finally {
            consumer.close();
            System.out.println("Consumer остановлен.");
        }
    }

    /**
     * Читает значение аргумента командной строки в форматах
     * {@code --name value} и {@code --name=value}. Если аргумент не задан,
     * возвращает значение по умолчанию.
     */
    private static String getArg(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals(name) && i + 1 < args.length) {
                return args[i + 1];
            }
            if (arg.startsWith(name + "=")) {
                return arg.substring(name.length() + 1);
            }
        }
        return defaultValue;
    }
}
