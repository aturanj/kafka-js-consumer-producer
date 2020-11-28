const { Kafka } = require("kafkajs");

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
            topic: "Log",
            messages: [{
                value: "This message belong to Log topic",
                partition: 0 //first partition
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