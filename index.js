const express = require('express');
const amqplib = require("amqplib");
const uuid = require('uuid');

const app = express();

const PORT = process.env.PORT || 5000;

const RMQ_URL = 'amqp://localhost'

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

app.delete('/delete-delayed-message/:id', async (req, res) => {
    const msgId = req.params.id;
    console.log('HIT DELETE ROUTE')
    await cancelScheduledMessages(msgId);
    res.send(200)
})





app.listen(PORT, () => {
    console.log(`SERVER STARTED ON PORT: ${PORT}`);
})


async function sendMessageToRabbitMQ() {
    const queueName = 'scheduler-queue-test';
    const message = 'testing ' + new Date();
    console.log(message);
    const opt = { credentials: amqplib.credentials.plain(USERNAME, PASSWORD) };
    const connection = await amqplib.connect(RMQ_URL, opt);
    const channel = await connection.createChannel();
    
    await channel.assertQueue(queueName, { durable: true });

    // // direct exchange
    // const exchangeName = 'test_direct_exchange';
    await channel.assertExchange('delay-exchange', 'x-delayed-message', { durable: true, arguments: {"x-delayed-type": "direct"} });
    // const args = {
    //     "x-delayed-type": "direct"
    // }
    // await channel.exchangeDeclare("delay-exchange", "x-delayed-message", true, false, args);
    
    // // bind the queue to the exchange
    const routingKey = 'delay_routing_key';
    await channel.bindQueue(queueName, 'delay-exchange', routingKey);


    // const data = await channel.sendToQueue(
    //     queueName,
    //     Buffer.from(message),
    //     { 
    //         persistent: true,
    //         expiration: 10000
    //         // headers: {
    //         //     "x-delay": 5000
    //         // } 
    //     }
    // );

    const messageId = uuid.v4(); // Generate a unique message ID
    const data = await channel.publish(
        'delay-exchange',
        routingKey,
        Buffer.from(message),
        { 
            persistent: true,
            headers: {
                "x-delay": 20000,
                'x-message-id': messageId
            } 
        }
    )

    console.log('data => ',data);
    console.log('messageId => ',messageId);

}

async function getMessageFromRabbitMQ() {
    const queueName = 'scheduler-queue-test';
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

// Cancel scheduled messages (set TTL to 0)
async function cancelScheduledMessages(messageId) {
    try {
        // const queueName = 'scheduler-queue-test';
        const connection = await amqplib.connect(RMQ_URL);
        const channel = await connection.createChannel();
        // Re-publish the message with a TTL of 0 to cancel it
        await channel.publish('delay-exchange', 'delay_routing_key', Buffer.from(''), {
            headers: { 'x-delay': 0, 'x-message-id': messageId }
        });
        console.log(`Cancelled scheduled message with ID: ${messageId}`);
    } catch (error) {
        console.error('Error cancelling scheduled message:', error);
    }
}