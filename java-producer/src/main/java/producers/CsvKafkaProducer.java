package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.opencsv.CSVReader;

import java.io.FileReader;
import java.util.Properties;

public class CsvKafkaProducer {

    public static void main(String[] args) throws Exception {
        // KAFKA producer configuration
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "stock";

        // Create KAFKA producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Read data from CSV file and publish to KAFKA topic
        // TODO: file location as command line arguments
        try (CSVReader csvReader = new CSVReader(new FileReader("data.csv"))) {
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                // Assuming each row in the CSV file is a message
                String message = String.join(",", line);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
                producer.send(record);
                System.out.println("Sent Packet...." + record.value());
                Thread.sleep(2000);
            }
        }

        // Close the producer
        producer.close();
    }
}
