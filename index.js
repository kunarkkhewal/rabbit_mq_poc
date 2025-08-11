const express = require('express');
const amqplib = require("amqplib");
const uuid = require('uuid');
const axios = require('axios');
const dotenv = require('dotenv');
dotenv.config();
const { performance } = require('perf_hooks');
const { rmqEventsAndRetry, handleMessageRetry } = require('./rmqUtils');

const app = express();

const PORT = process.env.PORT || 5005;

const queueName = 'kunark-poc'; 
// console.log('process.env.RMQ_USERNAME, process.env.RMQ_PASSWORD => ', process.env.RMQ_USERNAME, process.env.RMQ_PASSWORD)
const opt = { credentials: amqplib.credentials.plain(process.env.RMQ_USERNAME, process.env.RMQ_PASSWORD) };
const totalMessageCount = 10;
// const queueList = ['scheduler-queue-kunark', 'scheduler-queue-test']

let queueListResult;
let result = [];

app.post('/send', async (req, res) => {
    console.log('HIT SEND ROUTE')
    await sendMessageToRabbitMQ();
    res.sendStatus(200)
})

app.get('/get', async (req, res) => {
    console.log('HIT GET ROUTE')
    try {
        await getMessageFromRabbitMQ();
    } catch (error) {
        console.log('error in ROUTEgetMessageFromRabbitMQ', error);
    }
})

app.listen(PORT, () => {
    console.log(`SERVER STARTED ON PORT: ${PORT}`);
})

// const sendMessageToRabbitMQ = async () => {
//     let connection, channel; 
//     try {
//         // console.log(new Date(), ' - ', 'sendMessageToRabbitMQ - trying to queue: process.env.RMQ_URL, opt => ', queueName, process.env.RMQ_URL, opt);
//         connection = await amqplib.connect(process.env.RMQ_URL, opt);
//         // console.log("connection made");
//         channel = await connection.createChannel();
//         // console.log("channel created");
//         // const queueInfo = await channel.checkQueue(queueName);
//         console.log('queueInfo => ', queueInfo);
//         const assertQueueResponse = await channel.assertQueue(queueName, { durable: true, arguments: { "x-queue-type": "quorum", "x-delivery-limit": 10} });  //arguments: { "x-queue-type": "quorum", "x-delivery-limit": 10}
//         console.log('assertQueueResponse => ', assertQueueResponse);

//         const mgmtResponse = await axios.get(
//             `http://129.80.86.138:15672/api/queues/%2F/${queueName}`,
//             {
//               auth: {
//                 username: process.env.RMQ_USERNAME,
//                 password: process.env.RMQ_PASSWORD,
//               }
//             }
//           );

//         console.log('mgmtResponse => ', mgmtResponse);


//         const totalMsgConsumeTime = performance.now();
//         for (let i = 0; i < totalMessageCount; i++) {
//             console.log('i => ', i);
//             await channel.sendToQueue(
//                 queueName,
//                 Buffer.from(JSON.stringify(i)),
//                 { 
//                     persistent: true,
//                 }
//             );
//         }
//         // for (let i = 0; i < totalMessageCount; i++) {
//         //     console.log(`Sending message ${i} to queue: ${queueName}`);
//         //     const sent = await channel.sendToQueue(
//         //         queueName,
//         //         Buffer.from(JSON.stringify(i)),
//         //         { persistent: true }
//         //     );
//         //     console.log(`Message ${i} sent: ${sent}`);
//         // }

//         // Wait for all messages to be confirmed
//         // await channel.waitForConfirms();
//         // console.log("All messages confirmed!");


//         const finalTimeTaken = performance.now() - totalMsgConsumeTime;
//         console.log(`finalTimeTaken in ${totalMessageCount} msg send: ', ${finalTimeTaken}`);
//     } catch (error) {
//         console.log("sendMessageToRabbitMQ: ", error);
//     } finally {
//         // if (channel) channel.close();
//         // if (connection) connection.close();
//         channel.on('drain', () => {
//             console.log("Queue drained, closing connection...");
//             channel.close();
//             connection.close();
//         });
//     }
// }

const sendMessageToRabbitMQ = async () => {
    const message = {
        timestamp: Date.now(),
        message: 'hello world'
    }
    console.log(`message: ${JSON.stringify(message)}`)
    console.log(`sendMessageToRabbitMQ :: queueName : ${queueName}`)
    let connection;
    let channel;
    try {
        console.log(`process.env.RMQ_MQ_HOST : ${process.env.RMQ_MQ_HOST}`);
        console.log(`opt : ${JSON.stringify(opt)}`);
        connection = await amqplib.connect(process.env.RMQ_MQ_HOST, opt);
        channel = await connection.createChannel();
    } catch (error) {
        console.log("sendMessageToRabbitMQ :: Error in creating channel: ", error);
    }
    

    try {
        await channel.assertQueue(queueName, { durable: true, arguments: { "x-consumer-timeout": 60000 }}); // ,arguments: { "x-consumer-timeout": 60000, "x-queue-type": "quorum", "x-delivery-limit": 10}
        const optional = { persistent: true };
        await channel.sendToQueue(
            queueName,
            Buffer.from(JSON.stringify(message)),
            optional
        );
        console.log(`data sent to ${queueName}`);
    } catch (error) {
        console.error(`error in sendMessageToRabbitMQ: ${error}`);
    } finally {
        console.log(`Closing channel and connection`);
        await channel.close();
        await connection.close();
    }
}


const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// const getMessageFromRabbitMQ = async () => {
//     let connection, channel;
//     try {
//         connection = await amqplib.connect(process.env.RMQ_MQ_HOST, opt);
//         channel = await connection.createChannel();
//         await channel.assertQueue(queueName, { durable: true }); // arguments: { "x-queue-type": "quorum", "x-delivery-limit": 10}
//         console.log(new Date(), ' - ', 'Started Waiting for messages in queue: ', queueName);
//         let totalMsgConsumeTime, finalTimeTaken;
//         channel.prefetch(1500);
//         channel.consume(
//             queueName,
//             async (message) => {
//             try {
//                 if (message !== null) {
//                     const msg = JSON.parse(message.content.toString());
//                     console.log('Recieved: ', msg);
//                     if (msg == 0) {
//                         totalMsgConsumeTime = performance.now();
//                     }
//                     if (msg == totalMessageCount - 1) {
//                         finalTimeTaken = performance.now() - totalMsgConsumeTime;
//                     }
//                     // await delay(5000)
//                     console.log('ack: ', msg);
//                     channel.nack(message); // negative acknowledgement
//                     console.log(`finalTimeTaken in fetching ${totalMessageCount} mesg: ', ${finalTimeTaken}`);
//                     // setTimeout(() => {
//                     //     console.log('ack: ', msg);
//                     //     channel.ack(message);
//                     //     console.log(`finalTimeTaken in fetching ${totalMessageCount} mesg: ', ${finalTimeTaken}`);
                        
//                     // }, 500)
//                 }
//             } catch (error) {
//                 console.log('error: ', error);
//                 // await channel.nack(message);
//             }
//             },
//             { noAck: false }
//         );
//     } catch (error) {
//         console.error("Error in getMessageFromRabbitMQ: ", error);
//     } finally {
//         // channel.close();
//         // connection.close();
//     }
// }

const getMessageFromRabbitMQ = async (count = 0) => {
  let connection;
  let channel;
  // let count = 0;
  
    try {
      console.log(`calling getMessageFromRabbitMQ for queue: ${queueName}`);
      if(!connection) {
        connection = await amqplib.connect(process.env.RMQ_MQ_HOST, opt);
      }
      if (!channel) {
        channel = await connection.createChannel();
      }

      rmqEventsAndRetry(connection, channel, getMessageFromRabbitMQ, count);

      // if (count === 2) {
      await channel.assertQueue(queueName, { durable: true, arguments: { "x-consumer-timeout": 60000 }}); // ,arguments: { "x-consumer-timeout": 60000, "x-queue-type": "quorum", "x-delivery-limit": 10} // 60000 is the timeout for the consumer
      // } else {
        // await channel.assertQueue(queueName, { durable: true, arguments: { "x-queue-type": "quorum", }}); // ,arguments: { "x-consumer-timeout": 60000, "x-queue-type": "quorum", "x-delivery-limit": 10} // 60000 is the timeout for the consumer
      // }

      channel.consume(
        queueName,
        async (message) => {
          try {
            if (message !== null) {
              const brokeredMessage = JSON.parse(message.content.toString());
              console.log({brokeredMessage})
              // await delay(1800010);
              // await channel.ack(message);
              // console.log('deleted message from getMessageFromRabbitMQ');
              throw new Error('test error');
            } else {
              console.log('RabbitMQ Consumer cancelled by server');
            }
          } catch (error) {
            console.log(
              'error consuming RMQ message from getMessageFromRabbitMQ',
              error
            );
            // await channel.nack(message);
            await handleMessageRetry({
              channel,
              message,
              queueName,
              error,
              maxDeliveryAttempts: 3,
              shouldSendToDeadLetterQueue: true,
              shouldDeleteMessage: false,
            })
            // todo: add new function here to handle negative acknowledgement cases, 
            // either to requeue or to delete the message from the queue or send it to dead letter queue after certain number of retries, also to handle the number of retries for the message

            // // Just nack the message and requeue it (default behavior)
            // await channel.nack(message);

            // // Nack the message but don't requeue it
            // await channel.nack(message, false, false);

            // // Nack all messages up to this one and requeue them
            // await channel.nack(message, true, true);
          }
        },
        { noAck: false }
      );
      console.log(`registered consumer for RMQ -> ${queueName}`);
    } catch (error) {
      console.log(`error in getMessageFromRabbitMQ:: ${error}`);
    }
  };





  const getMessageFromRabbitMQWithExchange = async (count = 0) => {
    let connection;
    let channel;
    // let count = 0;
    const queueName = 'broadcast-queue-mehmood';   

      try {
        console.log(`calling getMessageFromRabbitMQ for queue: ${queueName}`);
        if(!connection) {
          connection = await amqplib.connect(process.env.RMQ_MQ_HOST, opt);
        }
        if (!channel) {
          channel = await connection.createChannel();
        }
  
        // if (count === 2) {
        // await channel.assertQueue(queueName, { durable: true, arguments: { "x-consumer-timeout": 60000 }}); // ,arguments: { "x-consumer-timeout": 60000, "x-queue-type": "quorum", "x-delivery-limit": 10} // 60000 is the timeout for the consumer
        // } else {
          // await channel.assertQueue(queueName, { durable: true, arguments: { "x-queue-type": "quorum", }}); // ,arguments: { "x-consumer-timeout": 60000, "x-queue-type": "quorum", "x-delivery-limit": 10} // 60000 is the timeout for the consumer
        // }
        const queueInfo = await getRMQManagementAPI(queueName);
        const consumerTag = queueInfo.consumer_details[0].consumer_tag; 
        console.log('consumerTag => ', consumerTag);
        channel.cancel(consumerTag);
        console.log(`registered consumer for RMQ -> ${queueName}`);
      } catch (error) {
        console.log(`error in getMessageFromRabbitMQ:: ${error}`);
      }
    };


const getRMQManagementAPI = async (queueName) => {
  const url = `${process.env.RMQ_MANAGEMENT_API_URL}/queues/%2F/${queueName}`;
  const response = await axios.get(url, {
    auth: {
      username: process.env.RMQ_USERNAME,
      password: process.env.RMQ_PASSWORD,
    }
  });

  console.log('response => ', response.data);
  return response.data;
}

const main = async () => {
  // await getRMQManagementAPI();
  // await getMessageFromRabbitMQ();
  await getMessageFromRabbitMQWithExchange();
}

// main();