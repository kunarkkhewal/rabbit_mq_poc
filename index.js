const express = require('express');
const amqplib = require("amqplib");
const uuid = require('uuid');
const dotenv = require('dotenv');
dotenv.config();

const app = express();

const PORT = process.env.PORT || 5000;

const queueName = 'scheduler-queue-test'

app.post('/send', async (req, res) => {
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

app.post('/send-batch', async (req, res) => {
    console.log('HIT SEND ROUTE')
    await sendMessagesInBatch();
    res.send(200)
})





app.listen(PORT, () => {
    console.log(`SERVER STARTED ON PORT: ${PORT}`);
})


async function sendMessageToRabbitMQ() {
    const queueName = 'scheduler-queue-test';
    const message = 'testing ' + new Date();
    console.log(message);
    const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
    const connection = await amqplib.connect(process.env.RMQ_URL, opt);
    const channel = await connection.createChannel();
    
    await channel.assertQueue(queueName, { durable: true });

    // // direct exchange
    // const exchangeName = 'test_direct_exchange';
    // await channel.assertExchange('delay-exchange', 'x-delayed-message', { durable: true, arguments: {"x-delayed-type": "direct"} });
    // const args = {
    //     "x-delayed-type": "direct"
    // }
    // await channel.exchangeDeclare("delay-exchange", "x-delayed-message", true, false, args);
    
    // // bind the queue to the exchange
    // const routingKey = 'delay_routing_key';
    // await channel.bindQueue(queueName, 'delay-exchange', routingKey);


    const data = await channel.sendToQueue(
        queueName,
        Buffer.from(message),
        { 
            persistent: true,
            // expiration: 10000
            // headers: {
            //     "x-delay": 5000
            // } 
        }
    );

    // const messageId = uuid.v4(); // Generate a unique message ID
    // const data = await channel.publish(
    //     'delay-exchange',
    //     routingKey,
    //     Buffer.from(message),
    //     { 
    //         persistent: true,
    //         headers: {
    //             "x-delay": 3600,
    //             'x-message-id': messageId
    //         },
    //         // expiration: 7200,
    //         messageId,
    //         // timestamp: 12345
    //     }
    // )

    console.log('data => ',data);
    // console.log('messageId => ',messageId);

}

async function getMessageFromRabbitMQ() {
    const queueName = 'scheduler-queue-test';
    const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
    const connection = await amqplib.connect(process.env.RMQ_URL, opt);
    
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
        const connection = await amqplib.connect(process.env.RMQ_URL);
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

async function sendMessagesInBatch() {
    const BigMsg = [[
        { content: "Message 1" },
        { content: "Message 2" },
        { content: "Message 3" }
    ],
    [
        { content: "Message 4" },
        { content: "Message 5" },
        { content: "Message 6" }
    ]];
    try {
      // Connect to RabbitMQ server
        const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
        const connection = await amqplib.connect(process.env.RMQ_URL, opt);
        // const connection = await amqplib.connect(process.env.RABBIT_MQ_HOST);
    
        // Create a channel
        const channel = await connection.createChannel();
    
        // Assert the queue exists, with durable option set to true for persistence
        await channel.assertQueue(queueName, { durable: true });
    
        // Start the transaction
        // await channel.sendToQueue(queueName, Buffer.from(''), { persistent: true });
        // await channel.publish('', queueName, Buffer.from(''), { persistent: true });
    
        for (const messages of BigMsg) {
            const channel1 = await connection.createChannel();
            for (const message of messages) {
                // Convert message to a buffer before sending
                const messageBuffer = Buffer.from(JSON.stringify(message));
                
                // Send message to the channel (inside the transaction)
                channel1.sendToQueue(queueName, messageBuffer, { persistent: true });
            }
            await channel1.close();
        }
        
    
        // Commit the transaction
        // await channel.publish('', queueName, Buffer.from(''), { persistent: true });
    
        // Close the channel and the connection
        await channel.close();
        await connection.close();
        
        console.log("Batch of messages sent to RabbitMQ");
    } catch (error) {
        console.error("Error sending batch of messages to RabbitMQ:", error);
    }
}

async function consumeMessages(queueName) {
  try {
    // Connect to RabbitMQ server
    const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
    const connection = await amqplib.connect(process.env.RMQ_URL, opt);

    // Create a channel
    const channel = await connection.createChannel();

    // Assert the queue exists
    await channel.assertQueue(queueName, { durable: true });

    // Define DLX and its arguments
    const dlxName = `${queueName}.dlx`;
    const dlxArguments = {
      'x-message-ttl': 30000, // TTL for messages in DLX (in milliseconds)
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': queueName // Requeue to original queue
    };

    // Assert the DLX
    await channel.assertExchange(dlxName, 'direct', { durable: true, arguments: dlxArguments });

    // Bind the DLX to the original queue
    await channel.bindQueue(queueName, dlxName, `${queueName}_dlx_routing_key`);

    // Consume messages from the queue
    await channel.consume(queueName, async (message) => {
      if (message !== null) {
        try {
          // Process the message
          await processMessage(message);

          // Acknowledge the message
          channel.ack(message);
        } catch (error) {
          console.error("Error processing message:", error);

          // Reject and requeue the message to DLX
          channel.nack(message, false, false);
        }
      }
    });
    
    console.log("Consumer started");

  } catch (error) {
    console.error("Error consuming messages from RabbitMQ:", error);
  }
}

async function processMessage(message) {
  // Simulate message processing
  await new Promise(resolve => setTimeout(resolve, 1000)); // Simulate processing time (1 second)
  
  // Simulate an error occurring randomly during processing
  if (Math.random() < 0.2) { // 20% chance of encountering an error
    throw new Error("Error during message processing");
  }

  console.log("Message processed:", message.content.toString());
}

// Example usage
// const queueName = "example_queue";
// consumeMessages(queueName);
