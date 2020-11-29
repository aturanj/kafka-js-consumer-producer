const { Kafka } = require("kafkajs");

const topicName = process.argv[2] || "Log2";
const partitionNumber = process.argv[3] || 0;

sendMessage();

async function sendMessage() {

    try {
        const kafka = new Kafka({
            clientId: "kafka_client_1",
            brokers: ["10.11.0.96:9092"]
        });

        const kafkaProducer = kafka.producer();
        console.log("Connecting Apache Kafka Producer...");

        await kafkaProducer.connect();
        console.log("Connected to Apache Kafka Producer");

        const result = await kafkaProducer.send({

            topic: topicName,

            messages: [{
                value: "This message belong to Log topic",
                partition: partitionNumber
            }]
        });

        console.log("Message sent", JSON.stringify(result));

        await kafkaProducer.disconnect();

    } catch (error) {
        console.log(error.message);
    } finally {
        process.exit(0);
    }
}