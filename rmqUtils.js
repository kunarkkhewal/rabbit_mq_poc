const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const rmqEventsAndRetry = async (connection, channel, functionToCall, count = 0) => {
    connection.on('error', (err) => {
        console.log(`RabbitMQ connection error (scheduler): ${err.message}`);
        // Connection will automatically try to reconnect or close
    });

    connection.on('close', () => {
        console.log('RabbitMQ connection closed (scheduler). Attempting to reconnect...');
        connection = null;
        channel = null;
        // Implement a delay before retrying
        // event should be cancelled by server first and then we will retry
        setTimeout(() => functionToCall(count + 1), 5000);
    });

    channel.on('error', (err) => {
        console.log(`RabbitMQ channel error (scheduler): ${err.message}`);
    });

    channel.on('close', async () => {
        console.log('RabbitMQ channel closed (scheduler). Will be recreated on next connection.');
        channel = null;
        if (connection && !connection.closeEmitted) { // Check if connection itself is not already closing
            console.log('Connection still open, attempting to recreate channel for scheduler.');
            console.log(`count: ${count}`);
            count++;
            if (count < 3) {
                await delay(count * 1000);
                functionToCall(count);
            } else {
                // todo: remove events
                throw new Error('RabbitMQ channel closed (scheduler). Will be recreated on next connection.');
            }
        }
    });
}


// // Just nack the message and requeue it (default behavior)
// await channel.nack(message);

// // Nack the message but don't requeue it
// await channel.nack(message, false, false);

// // Nack all messages up to this one and requeue them
// await channel.nack(message, true, true);



const getMessageContent = async (message) => {
    return Buffer.isBuffer(message.content) 
        ? message.content 
        : Buffer.from(JSON.stringify(message));
}

const DLX_CONFIG = {
    EXCHANGE: {
        NAME: 'dead.letter.exchange',
        TYPE: 'direct',
        OPTIONS: {
            durable: true,
            // internal: false // Set to true for internal-only exchange
        }
    },
    QUEUE: {
        DEFAULT_TTL: 1000 * 60 * 60 * 24 * 7, // 1 week
        MAX_LENGTH: 1000000,
        OVERFLOW_BEHAVIOUR: 'reject-publish',
        QUEUE_TYPE: 'classic'
    },
    RETRY: {
        MAX_ATTEMPTS: 10,
        BACKOFF_DELAYS: [1000, 5000, 10000, 30000], // Progressive retry delays
        ENABLE_EXPONENTIAL_BACKOFF: true
    },
    HEADERS: {
        DEATH_REASON: 'x-death-reason',
        ORIGINAL_QUEUE: 'x-original-queue',
        DEAD_LETTER_TIMESTAMP: 'x-dead-lettered-timestamp',
        ORIGINAL_MESSAGE_ID: 'x-original-message-id',
        ORIGINAL_TIMESTAMP: 'x-original-timestamp',
        DELIVERY_COUNT: 'x-delivery-count',
        LAST_ERROR: 'x-last-error',
        LAST_RETRY_TIMESTAMP: 'x-last-retry-timestamp'
    }
};

const setupDeadLetterExchange = async (channel, queueName) => {
    try {
        // Use a single, shared Dead Letter Exchange
        await channel.assertExchange(DLX_CONFIG.EXCHANGE.NAME, DLX_CONFIG.EXCHANGE.TYPE, DLX_CONFIG.EXCHANGE.OPTIONS);

        // Create queue-specific DLQ with the queue name as routing key
        const dlqName = `${queueName}.dlq`;
        await channel.assertQueue(dlqName, {
            durable: true,
            arguments: {
                "x-message-ttl": DLX_CONFIG.QUEUE.DEFAULT_TTL,
                "x-queue-type": DLX_CONFIG.QUEUE.QUEUE_TYPE,
                // "x-max-length": DLX_CONFIG.QUEUE.MAX_LENGTH,
                // "x-overflow": DLX_CONFIG.QUEUE.OVERFLOW_BEHAVIOUR
            }
        });

        // Bind the DLQ to the shared exchange using queue name as routing key
        await channel.bindQueue(dlqName, DLX_CONFIG.EXCHANGE.NAME, queueName);

        return { 
            dlxName: DLX_CONFIG.EXCHANGE.NAME, 
            dlqName, 
            routingKey: queueName // Using queue name as routing key
        };
    } catch (error) {
        console.log('error in setupDeadLetterExchange', error);
        throw error;
    }
}

const sendToDeadLetterQueue = async (channel, message, queueName, error) => {
    try {
        const { dlxName, dlqName, routingKey } = await setupDeadLetterExchange(channel, queueName);
        
        const messageContent = await getMessageContent(message);

        // Add dead-letter metadata
        const dlqHeaders = {
            ...message.properties.headers,
            [DLX_CONFIG.HEADERS.DEATH_REASON]: error?.message || 'Unknown error or no error',
            [DLX_CONFIG.HEADERS.ORIGINAL_QUEUE]: queueName,
            [DLX_CONFIG.HEADERS.DEAD_LETTER_TIMESTAMP]: new Date().toISOString(),
            [DLX_CONFIG.HEADERS.ORIGINAL_MESSAGE_ID]: message.properties.messageId,
            [DLX_CONFIG.HEADERS.ORIGINAL_TIMESTAMP]: message.properties.timestamp,
            [DLX_CONFIG.HEADERS.DELIVERY_COUNT]: 0, // Initial delivery count
            [DLX_CONFIG.HEADERS.LAST_ERROR]: error?.message || 'Unknown error or no error',
            [DLX_CONFIG.HEADERS.LAST_RETRY_TIMESTAMP]: new Date().toISOString()
        };

        // Publish to shared Dead Letter Exchange
        await channel.publish(
            dlxName,
            routingKey, // Using queue name as routing key
            messageContent,
            { 
                ...message.properties, 
                persistent: true, 
                headers: dlqHeaders,
                expiration: DLX_CONFIG.QUEUE.DEFAULT_TTL
            }
        );
        console.log(`Message published to DLQ: ${dlqName} and DLX: ${dlxName} with routing key: ${routingKey}`);
        await channel.ack(message);
        return;
    } catch (error) {
        console.log('error in sendToDeadLetterQueue', error);
        throw error;
    }
}

const calculateBackoff = (attemptCount) => {
    if (!DLX_CONFIG.RETRY.ENABLE_EXPONENTIAL_BACKOFF) {
        return DLX_CONFIG.RETRY.BACKOFF_DELAYS[
            Math.min(attemptCount, DLX_CONFIG.RETRY.BACKOFF_DELAYS.length - 1)
        ];
    }
    
    const baseDelay = DLX_CONFIG.RETRY.BACKOFF_DELAYS[0];
    return Math.min(
        baseDelay * Math.pow(2, attemptCount - 1),
        DLX_CONFIG.RETRY.BACKOFF_DELAYS[DLX_CONFIG.RETRY.BACKOFF_DELAYS.length - 1]
    );
};

const requeueMessage = async (channel, message, queueName, error, deliveryCount, headers) => {
    try {
        const updatedHeaders = {
            ...headers,
            [DLX_CONFIG.HEADERS.DELIVERY_COUNT]: deliveryCount + 1,
            [DLX_CONFIG.HEADERS.LAST_ERROR]: error?.message || 'Unknown error or no error',
            [DLX_CONFIG.HEADERS.LAST_RETRY_TIMESTAMP]: new Date().toISOString()
        };
    
        const messageContent = await getMessageContent(message);
        // const backoffDelay = calculateBackoff(deliveryCount + 1);
    
        await channel.sendToQueue(
            queueName,
            messageContent,
            { 
                ...message.properties, 
                persistent: true, 
                headers: updatedHeaders,
                // expiration: backoffDelay // Apply backoff delay
            }
        );
    
        console.log(`Message requeued with after ${deliveryCount + 1} attempts`); // todo: update this to log the message id and the queue name
        await channel.ack(message);
    } catch (error) {
        console.error('Error in requeueMessage:', error);
        throw error;
    }
}

const logger = {
    info: (message, meta = {}) => {
        console.log(JSON.stringify({ level: 'info', message, timestamp: new Date().toISOString(), ...meta }));
    },
    error: (message, error, meta = {}) => {
        console.error(JSON.stringify({
            level: 'error',
            message,
            error: {
                name: error?.name,
                message: error?.message,
                stack: error?.stack
            },
            timestamp: new Date().toISOString(),
            ...meta
        }));
    }
};

const validateMessage = (message) => {
    if (!message) throw new Error('Message is required');
    if (!message.content) throw new Error('Message content is required');
    if (!message.properties) throw new Error('Message properties are required');
};

const handleMessageRetry = async (params) => {
    try {
        const { 
            channel, 
            message, 
            queueName, 
            error = { message: null }, 
            maxDeliveryAttempts = DLX_CONFIG.RETRY.MAX_ATTEMPTS, 
            shouldSendToDeadLetterQueue = false, 
            shouldDeleteMessage = false 
        } = params;

        // Validate inputs
        // if (!channel) throw new Error('Channel is required');
        // if (!queueName) throw new Error('Queue name is required');
        // validateMessage(message);

        const headers = message?.properties?.headers || {};
        let deliveryCount = headers[DLX_CONFIG.HEADERS.DELIVERY_COUNT] || 0;

        logger.info('Processing message retry', {
            queueName,
            deliveryCount,
            maxDeliveryAttempts,
            shouldSendToDeadLetterQueue,
            shouldDeleteMessage,
            error: error?.message
        });

        if (deliveryCount >= maxDeliveryAttempts) {
            if (shouldSendToDeadLetterQueue) {
                await sendToDeadLetterQueue(channel, message, queueName, error);
            } else if (shouldDeleteMessage) {
                logger.info('Deleting message after max retries', { queueName, deliveryCount });
                await channel.nack(message, false, false);
            } else {
                await requeueMessage(channel, message, queueName, error, deliveryCount, headers);
            }
        } else {
            await requeueMessage(channel, message, queueName, error, deliveryCount, headers);
        }
    } catch (error) {
        logger.error('Failed to handle message retry', error, {
            queueName: params?.queueName,
            messageId: params?.message?.properties?.messageId
        });
        
        // Ensure message is nack'd in case of errors
        if (params?.channel && params?.message) {
            try {
                await params.channel.nack(params.message);
            } catch (nackError) {
                logger.error('Failed to nack message after error', nackError);
            }
        }
        
        // throw error; // Re-throw for upstream handling
    }
}

// Export with proper TypeScript-like documentation
module.exports = {
    rmqEventsAndRetry,
    handleMessageRetry,
    setupDeadLetterExchange,
    DLX_CONFIG
};