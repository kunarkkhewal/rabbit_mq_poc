const express = require('express');
const amqplib = require("amqplib");
const uuid = require('uuid');
const axios = require('axios');
const dotenv = require('dotenv');
dotenv.config();

const app = express();

const PORT = process.env.PORT || 5000;

const queueName = 'scheduler-queue-kunark'
const queueList = ['scheduler-queue-kunark', 'scheduler-queue-test']

let queueListResult;
let result = [];

app.post('/send', async (req, res) => {
    console.log('HIT SEND ROUTE')
    // await sendMessageToRabbitMQ();
    await sendToDynamicQueue();
    res.send(200)
})

app.get('/get', async (req, res) => {
    console.log('HIT GET ROUTE')
    // await getMessageFromRabbitMQ();
    queueListResult = await getFromDynamicQueue();
    // await getFromDynamicQueue2();
    setTimeout(async () => {
        await handleDynamicQueue();
    }, 15000);
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

let connection
(async () => {
    const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
    connection = await amqplib.connect(process.env.RMQ_URL, opt);
    console.log(' ---- CONNECTION MADE')
})()


async function publishDelayedMessage() {
    const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
    const connection = await amqplib.connect(process.env.RMQ_URL, opt);
    const channel = await connection.createChannel();

    const queueName = 'scheduler-queue-test';
    const message = 'testing ' + new Date();
    const delayInMilliseconds = 60000; // 1 minute delay

    await channel.assertQueue(queueName, { durable: true });

    const options = { expiration: delayInMilliseconds.toString() };
    channel.sendToQueue(queueName, Buffer.from(message), options);

    console.log('Message published with delay.');
    await channel.close();
    await connection.close();
}

// publishDelayedMessage().catch(console.error);


async function sendMessageToRabbitMQ() {
    // const queueName = 'scheduler-queue-test';
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
    // const queueName = 'scheduler-queue-test';
    const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
    const connection = await amqplib.connect(process.env.RMQ_URL, opt);
    // console.log(' ---- connection => ', connection)
    
    const channel = await connection.createChannel();
    // console.log(' ---- channel => ', channel)

    let count = 1; 
    await channel.assertQueue(queueName, { durable: true });

    const consumer = await channel.consume(queueName, (msg) => {
        if (msg !== null) {
            console.log('Recieved:', msg.content.toString());
            channel.ack(msg);
            if (count == 2) {
                channel.close();
            }
            console.log('count => ', count)
            count++;

        } else {
            console.log('Consumer cancelled by server');
        }
    })
    console.log(' ---- consumer => ', consumer)
}


async function sendToDynamicQueue() {
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

        for (let i = 0; i < queueList.length; i++) {
            // Assert the queue exists, with durable option set to true for persistence
            await channel.assertQueue(queueList[i], { durable: true });
                            
            for (const messages of BigMsg) {
                const channel1 = await connection.createChannel();
                for (const message of messages) {
                    // Convert message to a buffer before sending
                    const messageBuffer = Buffer.from(JSON.stringify(message));
                    
                    // Send message to the channel (inside the transaction)
                    channel.sendToQueue(queueList[i], messageBuffer, { persistent: true });
                }
                await channel1.close();
            }
            console.log("Batch of messages sent to RabbitMQ");
        }

        // queueList.map(async (queueName) => {
            
        // })
        await channel.close();
        await connection.close();
    } catch (error) {
        console.error("Error sending batch of messages to RabbitMQ:", error);
    }
}

// async function getFromDynamicQueue() {
//     // const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
//     // const connection = await amqplib.connect(process.env.RMQ_URL, opt);
//     let result = []

//     for (let i = 0; i < queueList.length; i++) {
//         const channel = await connection.createChannel();
//         // console.log(' ---- channel => ', channel)
    
//         let count = 1; 
//         await channel.assertQueue(queueList[i], { durable: true });
    
//         channel.consume(queueList[i], async (msg) => {
//             if (msg !== null) {
//                 console.log('Recieved:', msg.content.toString());
//                 channel.ack(msg);
//                 // if (count == 2) {
//                 //     channel.close();
//                 // }
//                 console.log('count => ', count)
//                 count++;
//                 if (await checkIfQueueConsumerCanBeDeleted(queueList[i])) {
//                     setTimeout(async () => {
//                         console.log('closing consumer')
//                         await channel.close();
//                     }, 2000)
//                 }
//             } else {
//                 console.log('Consumer cancelled by server');
//             }
//         })
//         console.log(' ---- consumer registered on => ', queueList[i])
//         result.push({queueNAme: queueList[i], channel})
//     }

//     // const result = queueList.map(async queueName => {
        
//     // })

//     console.log(' ---- result => ', result)
// }

async function getFromDynamicQueue() {
    // await getFromDynamicQueue2();
    

    for (let i = 0; i < queueList.length; i++) {
        const channel = await connection.createChannel();
        let count = 1;
        let isChannelCloseRequestMade = false;

        await channel.assertQueue(queueList[i], { durable: true });

        channel.consume(queueList[i], async (msg) => {
            // let isChannelCloseRequestMade = false;
            if (msg !== null) {
                console.log('queueList[i], Received:', queueList[i], msg.content.toString());
                await channel.ack(msg);
                
                console.log('queueList[i] count => ', queueList[i], count)
                count++;
                
                
                // if (await checkIfQueueConsumerCanBeDeleted(queueList[i])) {
                //     await deleteDynamicQueue(queueList[i]);
                // }
            } else {
                console.log('Consumer cancelled by server');
            }
        }, { noAck: false }).catch((err) => {
            console.error('Error in channel.consume:', err);
        });

        console.log(' ---- consumer registered on => ', queueList[i]);
        result.push({ queueName: queueList[i], channel, isChannelCloseRequestMade: false });
    }

    // console.log(' ---- result => ', result);
    return result;
}

async function deleteDynamicQueue(queue) {
    const index = result.findIndex(listItem => listItem.queueName === queue)
    let queueResult;
    if (index > -1) {
        queueResult = result[index];
    } else {
        return 'already deleted'
    }
    console.log('Closing consumer', result[index].queueName);
    // Clear the consumer and close the channel after a delay
    // setTimeout(async () => {
        try {
            console.log('BEFORE IF isChannelCloseRequestMade => ', queueResult.isChannelCloseRequestMade)
            if (!queueResult.isChannelCloseRequestMade) {
                console.log('INSIDE IF')
                result[index].isChannelCloseRequestMade = true
                console.log('INSIDE IF AFTER MAKING TRUE')
                // try {
                await queueResult.channel.close();
                console.log(`Channel closed for queue ${queueResult.queueName}`);

                const index2 = result.findIndex(listItem => listItem.queueName === queueResult.queueName)
                console.log('below index => ', index2)
                result.splice(index2, 1);
                // } catch (error) {
                //     console.log("ERROR")
                // }
                
            }
        } catch (err) {
            console.error('Error closing channel:', err);
        }
    // }, 2000); // Delay as needed
}

async function handleDynamicQueue() {
    queueListResult.map(async (listItem) => {
        // await checkIfQueueConsumerCanBeDeleted(listItem.queueName)
        if (await checkIfQueueConsumerCanBeDeleted(listItem.queueName)) {
            console.log(' ----- deleting ')
            await deleteDynamicQueue(listItem.queueName);
        }
    })
}

async function getFromDynamicQueue2() {
    let result = [];

    for (let i = 0; i < queueList.length; i++) {
        const channel = await connection.createChannel();
        let count = 1;

        await channel.assertQueue(queueList[i], { durable: true });

        channel.consume(queueList[i], async (msg) => {
            if (msg !== null) {
                console.log('getFromDynamicQueue2, queueList[i], Received:', queueList[i], msg.content.toString());
                channel.ack(msg);
                
                console.log('getFromDynamicQueue2, queueList[i] count => ', queueList[i], count)
                count++;
                
                // if (await checkIfQueueConsumerCanBeDeleted(queueList[i])) {
                //     console.log('Closing consumer', queueList[i]);
                //     // Clear the consumer and close the channel after a delay
                //     // setTimeout(async () => {
                //         try {
                //             await channel.close();
                //             console.log(`Channel closed for queue ${queueList[i]}`);
                //         } catch (err) {
                //             console.error('Error closing channel:', err);
                //         }
                //     // }, 2000); // Delay as needed
                // }
            } else {
                console.log('getFromDynamicQueue2, Consumer cancelled by server');
            }
        }, { noAck: false }).catch((err) => {
            console.error('getFromDynamicQueue2, Error in channel.consume:', err);
        });

        console.log(' ---- getFromDynamicQueue2, consumer registered on => ', queueList[i]);
        result.push({ queueName: queueList[i], channel });
    }

    console.log(' ---- getFromDynamicQueue2, result => ', result);
}


async function checkIfQueueConsumerCanBeDeleted(queueName) {
    const channel = await connection.createChannel();
    try {
        const queue = await channel.checkQueue(queueName);
        console.log(' ------ queue info => ', queue)
        const canBeDeleted = queue.messageCount === 0 && queue.consumerCount !== 0;
        await channel.close();
        return canBeDeleted;
    } catch (error) {
        console.log(' -- error in checkIfQueueConsumerCanBeDeleted => ', error);
        await channel.close();
        return false;
    }
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
                channel.sendToQueue(queueName, messageBuffer, { persistent: true });
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

// async function sendMessagesInBatch() {
//     const BigMsg = [
//         [
//             { content: "Message 1" },
//             { content: "Message 2" },
//             { content: "Message 3" }
//         ],
//         [
//             { content: "Message 4" },
//             { content: "Message 5" },
//             { content: "Message 6" }
//         ]
//     ];

//     try {
//         // Connect to RabbitMQ server
//         const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
//         const connection = await amqplib.connect(process.env.RMQ_URL, opt);

//         // Create a channel
//         const channel = await connection.createChannel();

//         // Assert the queue exists, with durable option set to true for persistence
//         await channel.assertQueue(queueName, { durable: true });

//         for (const messages of BigMsg) {
//             for (const message of messages) {
//                 // Convert message to a buffer before sending
//                 const messageBuffer = Buffer.from(JSON.stringify(message));

//                 // Send message to the queue with persistent option
//                 await channel.sendToQueue(queueName, messageBuffer, { persistent: true });
//             }
//         }

//         // Close the channel and the connection
//         await channel.close();
//         await connection.close();

//         console.log("Batch of messages sent to RabbitMQ");
//     } catch (error) {
//         console.error("Error sending batch of messages to RabbitMQ:", error);
//     }
// }


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

// async function getQueueRuntimeProperties() {
//     try {
//         const username = process.env.USERNAME;
//         const password = process.env.PASSWORD;
//         const authHeader = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
    
//         const response = await axios.get(`http://129.80.82.16:15672/api/queues/%2F/${queueName+'-test'}`, {
//             headers: {
//                 'Authorization': authHeader
//             }
//         });
//         console.log('data => ', response.data);
//     } catch (error) {
//         console.log('error => ', error)
//         if (error.response && error.response.status === 404) {
//             console.log(`The queue '${queueName}' does not exist.`);
//             return null;
//         } else {
//             throw error;
//         }
//     }
// }

// async function getQueueRuntimeProperties() {
//     const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
//     const connection = await amqplib.connect(process.env.RMQ_URL, opt);
//     const channel = await connection.createChannel();
    
//     try {

//         const createQueueResponse = await channel.assertQueue(queueName+'', { durable: true });
//         console.log(' ---- createQueueResponse => ', createQueueResponse)
        
//         const queue = await channel.checkQueue(queueName+"-add");
//         console.log(' ---- QUEUE info => ', queue)
//         const isExist = !!queue;

//         console.log(`Checking if queue exists Queue {${queueName}} found: ${isExist}`);

//         return {isExist, queue};
//         // const username = process.env.USERNAME;
//         // const password = process.env.PASSWORD;
//         // const authHeader = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
    
//         // const response = await axios.get(`http://129.80.82.16:15672/api/queues/%2F/${queueName+'-test'}`, {
//         //     headers: {
//         //         'Authorization': authHeader
//         //     }
//         // });
//         // console.log('data => ', response.data);
//     } catch (error) {
//         console.log('error => ', error)
//         // if (error.response && error.response.status === 404) {
//         //     console.log(`The queue '${queueName}' does not exist.`);
//         //     return null;
//         // } else {
//         //     throw error;
//         // }
//         if (error.code === 404) { // Not found
//             console.log(`Queue {${queueName}} does not exist.`);
//             return false;
//         } else {
//             console.log('isQueueExist', error.message);
//             // throw error;
//         }
//     } finally {
//         // channel.close()
//         connection.close();
//     }
// }


async function getQueueRuntimeProperties() {
    let channel;

    try {
        // const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
        // connection = await amqplib.connect(process.env.RMQ_URL, opt);
        
        // Handle connection errors
        // connection.on('error', (err) => {
        //     console.error('Connection error:', err);
        // });

        channel = await connection.createChannel();
        
        // Handle channel errors
        channel.on('error', (err) => {
            console.error('Channel error:', err);
        });

        const createQueueResponse = await channel.assertQueue(queueName, { durable: true });
        console.log(' ---- createQueueResponse => ', createQueueResponse);

        const queue = await channel.checkQueue(queueName + "");
        console.log(' ---- QUEUE info => ', queue);
        const isExist = !!queue;

        console.log(`Checking if queue exists Queue {${queueName}} found: ${isExist}`);

        return { isExist, queue };
    } catch (error) {
        console.log('error => ', error);
        if (error.code === 404) { // Not found
            console.log(`Queue {${queueName}} does not exist.`);
            return false;
        } else {
            console.log('isQueueExist', error.message);
            throw error;
        }
    } finally {
        if (channel) {
            try {
                await channel.close();
            } catch (err) {
                console.error('Error closing channel:', err);
            }
        }
        if (connection) {
            try {
                await connection.close();
            } catch (err) {
                console.error('Error closing connection:', err);
            }
        }
    }
}

isQueueExist = async (queueName) => {
    // const logInfo = {
    //   functionName: 'isQueueExist',
    //   userId: null,
    //   patientId: null,
    // };
    // const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
    // const connection = await amqplib.connect(process.env.RMQ_URL, opt);
    const channel = await connection.createChannel();
    try {
        const queue = await channel.checkQueue(queueName);
        const isExist = !!queue;

        // logger.info(
        //   getServiceLogMessage(
        //     logInfo,
        //     'Checking if queue exists',
        //     `Queue {${queueName}} found: ${isExist}`
        //   )
        // );

        return isExist;
    } catch (error) {
        if (error.code === 404) { // Not found
            console.log(`Queue {${queueName}} does not exist.`);
        } else {
            console.log('isQueueExist', error.message);
        }
        return false;
    }
  }

async function getQueueCurrentProperties() {
    const managementApiUrl = 'http://129.80.82.16:15672/api';
    const managementApiAuth = {
        username: process.env.USERNAME,
        password: process.env.PASSWORD
    };
    try {
        const response = await axios.get(
            `${managementApiUrl}/queues/%2F/${queueName}`,
            {
                auth: managementApiAuth
            }
        );
        let data = response.data;
        console.log(response);
        // console.log(JSON.stringify(data, null, 2));

        data = {
          totalMessageCount: data.messages,
          activeMessageCount: data.messages,
          deadLetterMessageCount: 0,
          scheduledMessageCount: 0,
          consumer: data.consumers
        }

        console.log(JSON.stringify(data, null, 2));

        return data;
    } catch (error) {
        console.log('Error in getQueueCurrentProperties', error.message);
        return {};
    }
}

// async getQueueCurrentProperties(queueName: string): Promise<QueueRuntimeProperties> {
//     try {
//         const response = await axios.get(
//             `${this.managementApiUrl}/queues/%2F/${queueName}`,
//             {
//                 auth: this.managementApiAuth
//             }
//         );
//         let data = response.data;
//         // logger.info(JSON.stringify(data));

//         data = {
//           totalMessageCount: data.messages,
//           activeMessageCount: data.messages,
//           deadLetterMessageCount: 0,
//           scheduledMessageCount: 0,
//         }

//         return data as QueueRuntimeProperties;
//     } catch (error) {
//         logger.error('Error in getQueueCurrentProperties', error.message);
//         return {} as QueueRuntimeProperties;
//     }
//   }

async function deleteQueue() {
    try {
        const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
        const connection = await amqplib.connect(process.env.RMQ_URL, opt);
        const channel = await connection.createChannel();
        const result = await channel.deleteQueue('scheduler-queue-test-test');
        console.log('result delete => ', result)
    } catch (error) {
        console.log("ERROR EOOOR = > ", error)
    }
    
}

setTimeout(async () => {
    console.log("CALLING getQueueRuntimeProperties")
    try {
        console.log('result => ', await deleteQueue());
    } catch (error) {
        console.log('BIG ERROR')
    }
}, 5000)


// import amqp from 'amqplib';
// import logger from 'your-logger';

async function sendMessagesToQueue() {
    const messages = [
        { 
            body: "Message 1",
            scheduledEnqueueTimeUtc: new Date()
        },
        { 
            body: "Message 2",
            scheduledEnqueueTimeUtc: new Date()
        },
        { 
            body: "Message 3",
            scheduledEnqueueTimeUtc: new Date()
        },
        { 
            body: "Message 4",
            scheduledEnqueueTimeUtc: new Date()
        },
        { 
            body: "Message 5",
            scheduledEnqueueTimeUtc: new Date()
        },
        { 
            body: "Message 6",
            scheduledEnqueueTimeUtc: new Date()
        }
    ];

    const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
    const connection = await amqplib.connect(process.env.RMQ_URL, opt);
    const channel = await connection.createChannel();

    try {
        await channel.assertQueue(queueName, { durable: true });

        for (const message of messages) {
            channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message.body)), {
                persistent: true,
                headers: {
                    'x-delay': message.scheduledEnqueueTimeUtc ? 60000 : 0
                }
            });
        }

        console.log(`Sent ${messages.length} messages to queue ${queueName}`);
    } catch (error) {
        console.log(error.message);
        throw error;
    } finally {
        await channel.close();
        await connection.close();
    }
}

// Example usage
// const messages = batchSendData.map(data => ({
//     body: data.body,
//     scheduledEnqueueTimeUtc: data.scheduledEnqueueTimeUtc
// }));

// sendMessagesToQueue(messages, 'your-queue-name').catch(console.error);



// const amqp = require('amqplib');

async function consume() {
    try {
        // const connection = await amqp.connect('amqp://localhost');
        const opt = { credentials: amqplib.credentials.plain(process.env.USERNAME, process.env.PASSWORD) };
        connection = await amqplib.connect(process.env.RMQ_URL, opt);
        
        const channel = await connection.createChannel();

        // Handle channel errors
        channel.on('error', (err) => {
            console.error('Channel error:', err);
        });


        const queue = queueName;

        await channel.assertQueue(queue, {
            durable: true
        });

        console.log(`Waiting for messages in ${queue}. To exit press CTRL+C`);

        channel.consume(queue, async (msg) => {
            if (msg !== null) {
                try {
                    const content = msg.content.toString();
                    console.log("Received:", content);

                    // Set a timeout to simulate long message processing
                    const processingTime = 60000; // 1 minute

                    const renewInterval = 10000; // 10 seconds

                    const heartbeat = setInterval(() => {
                        // Send a heartbeat by acknowledging and requeueing the message
                        channel.nack(msg, false, true);
                        console.log("Message lock renewed");
                    }, renewInterval);

                    // Simulate message processing
                    await processMessage(content, processingTime);

                    clearInterval(heartbeat);

                    // Acknowledge the message after successful processing
                    channel.ack(msg);
                    console.log("Message processed successfully");
                } catch (error) {
                    console.error("Processing failed:", error.message);

                    // Negative acknowledge and requeue the message
                    channel.nack(msg, false, true);
                }
            }
        }, { noAck: false });
    } catch (error) {
        console.error("Error in consumer setup:", error.message);
    }
}

async function processMessage(message, duration) {
    // Simulate long message processing
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            // Randomly simulate success or failure
            if (Math.random() > 0.5) {
                resolve();
            } else {
                reject(new Error("Random processing error"));
            }
        }, duration);
    });
}

// consume();
