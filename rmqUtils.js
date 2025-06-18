const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export const rmqEventsAndRetry = async (connection, channel, functionToCall, count = 0) => {
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