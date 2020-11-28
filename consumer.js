const { Kafka } = require("kafkajs");

getMessages();

async function getMessages() {

    try {
        const kafka = new Kafka({
            clientId: "kafka_client_1",
            brokers: ["10.11.0.96:9092"]
        });

        const kafkaConsumer = kafka.consumer({
            groupId: "consumer_group_1"
        });
        console.log("Connecting Apache Kafka Consumer...");

        await kafkaConsumer.connect();
        console.log("Connected to Apache Kafka Consumer");

        await kafkaConsumer.subscribe({
            topic: "Log",
            fromBeginning: true
        });

        await kafkaConsumer.run({
            eachMessage: async result => {
                console.log(`Message: ${result.message.value} Partition => ${result.partition}`);
            }
        });

    } catch (error) {
        console.log(error.message);
    }
}