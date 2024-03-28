const express = require('express');
const amqplib = require("amqplib");

const app = express();

const PORT = process.env.PORT || 5000;

app.get('/send', async (req, res) => {
    console.log('HIT SEND ROUTE')
    await sendMessageToRabbitMQ();
    res.send(200)
})

app.get('/get', async (req, res) => {
    console.log('HIT GET ROUTE')
    await getMessageFromRabbitMQ();
    res.send(200)
})



app.listen(PORT, () => {
    console.log(`SERVER STARTED ON PORT: ${PORT}`);
})


async function sendMessageToRabbitMQ() {
    const queueName = 'queue-test';
    const message = 'testing ' + new Date();
    console.log(message);
    const opt = { credentials: amqplib.credentials.plain(USERNAME, PASSWORD) };
    const connection = await amqplib.connect(RMQ_URL, opt);
    const channel = await connection.createChannel();
    
    await channel.assertQueue(queueName, { durable: true });

    // // direct exchange
    // const exchangeName = 'test_direct_exchange';
    // await channel.assertExchange(exchangeName, 'direct', { durable: true });
    
    // // bind the queue to the exchange
    // const routingKey = 'test_routing_key';
    // await channel.bindQueue(queueName, exchangeName, routingKey);


    const data = await channel.sendToQueue(
        queueName,
        Buffer.from(message),
        { persistent: true }
    );

    console.log('data => ',data);
}

async function getMessageFromRabbitMQ() {
    const queueName = 'queue-test';
    const opt = { credentials: amqplib.credentials.plain(USERNAME, PASSWORD) };
    const connection = await amqplib.connect(RMQ_URL, opt);
    
    const channel = await connection.createChannel();
    await channel.assertQueue(queueName, { durable: true });

    channel.consume(queueName, (msg) => {
        if (msg !== null) {
            console.log('Recieved:', msg.content.toString());
            channel.ack(msg);
        } else {
            console.log('Consumer cancelled by server');
        }
    })
}