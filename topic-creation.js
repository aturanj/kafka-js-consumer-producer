const { Kafka } = require("kafkajs");

createTopic();

async function createTopic() {

    try {
        const kafka = new Kafka({
            clientId: "kafka_client_1",
            brokers: ["10.11.0.96:9092"]
        });

        const kafkaAdmin = kafka.admin();
        console.log("Connecting Apache Kafka...");

        await kafkaAdmin.connect();
        console.log("Connected to Apache Kafka");

        await kafkaAdmin.createTopics({
            topics: [
                {
                    topic: "Log",
                    numPartitions: 1
                },
                {
                    topic: "User",
                    numPartitions: 2
                }
            ]
        });

        console.log("Topics Created");

        await kafkaAdmin.disconnect();

    } catch (error) {
        console.log(error.message);
    } finally {
        process.exit(0);
    }
}