const MAX_RETRIES = 3; // Maximum number of retries before sending to DLQ
const RETRY_DELAY_MS = 5000; // Delay between retries in milliseconds

/**
 * Handles message retry logic and DLQ routing
 * @param {Object} channel - RabbitMQ channel
 * @param {Object} message - RabbitMQ message object
 * @param {string} queueName - Name of the original queue
 * @param {Error} error - Error that caused the nack
 * @returns {Promise<void>}
 */
const handleMessageRetry = async (channel, message, queueName, error) => {
    try {
        // Get retry count from message headers or initialize to 0
        const headers = message.properties.headers || {};
        const retryCount = headers['x-retry-count'] || 0;

        if (retryCount >= MAX_RETRIES) {
            // Send to Dead Letter Queue
            await sendToDeadLetterQueue(channel, message, queueName, error);
            // Acknowledge the message to remove it from the original queue
            await channel.ack(message);
            console.log(`Message exceeded retry limit (${MAX_RETRIES}), sent to DLQ`);
            return;
        }

        // Increment retry count
        const updatedHeaders = {
            ...headers,
            'x-retry-count': retryCount + 1,
            'x-original-queue': queueName,
            'x-last-error': error.message,
            'x-last-retry-timestamp': new Date().toISOString()
        };

        // Republish the message to the same queue with updated headers
        await channel.publish(
            '',  // Default exchange
            queueName,
            message.content,
            {
                persistent: true,
                headers: updatedHeaders,
                // Preserve original message properties except headers
                ...message.properties,
                headers: updatedHeaders
            }
        );

        // Acknowledge the original message
        await channel.ack(message);
        console.log(`Message requeued for retry ${retryCount + 1}/${MAX_RETRIES}`);

    } catch (retryError) {
        console.error('Error in handleMessageRetry:', retryError);
        // If we can't handle the retry, nack the message without requeue
        await channel.nack(message, false, false);
    }
};

/**
 * Sends a message to the Dead Letter Queue
 * @param {Object} channel - RabbitMQ channel
 * @param {Object} message - RabbitMQ message object
 * @param {string} originalQueue - Name of the original queue
 * @param {Error} error - Error that caused the message to be dead-lettered
 */
const sendToDeadLetterQueue = async (channel, message, originalQueue, error) => {
    const dlqName = `${originalQueue}-dlq`;

    try {
        // Assert DLQ exists
        await channel.assertQueue(dlqName, {
            durable: true,
            arguments: {
                "x-queue-type": "quorum",  // Using quorum queue type for consistency
                "x-max-length": 10000,     // Limit DLQ size
                "x-message-ttl": 1000 * 60 * 60 * 24 * 7  // 7 days TTL
            }
        });

        // Add dead-letter metadata
        const dlqHeaders = {
            ...message.properties.headers,
            'x-death-reason': error.message,
            'x-original-queue': originalQueue,
            'x-dead-lettered-timestamp': new Date().toISOString()
        };

        // Publish to DLQ
        await channel.publish(
            '',  // Default exchange
            dlqName,
            message.content,
            {
                persistent: true,
                headers: dlqHeaders,
                // Preserve original message properties except headers
                ...message.properties,
                headers: dlqHeaders
            }
        );

        console.log(`Message sent to DLQ: ${dlqName}`);
    } catch (dlqError) {
        console.error('Error sending message to DLQ:', dlqError);
        throw dlqError;  // Propagate error to caller
    }
};

module.exports = {
    handleMessageRetry,
    sendToDeadLetterQueue,
    MAX_RETRIES,
    RETRY_DELAY_MS
}; 