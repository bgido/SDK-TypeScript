import * as dotenv from 'dotenv';
dotenv.config();

// Access environment variables
const AccountID: number = parseInt(process.env.AccountID || '223671990', 10);
const Host: string = process.env.Host || '';
const User: string = process.env.User || '';
const Pass: string = process.env.Pass || '';

import { memphis, Memphis, Message } from 'memphis-dev';

(async function () {
    let memphisConnection: Memphis | null = null;

    try {
        memphisConnection = await memphis.connect({
            host: Host,
            username: User,
            password: Pass,
            accountId: AccountID
        });

        const consumer = await memphisConnection.consumer({
            stationName: 'eventlog-1',
            consumerName: 'consumer-TS-Barak-1',
            consumerGroup: "TS-Gido-1",
            pullIntervalMs: 100, // defaults to 1000
            batchSize: 1000, // defaults to 10
            batchMaxTimeToWaitMs: 5000, // defaults to 5000
            maxAckTimeMs: 30000, // defaults to 30000
            maxMsgDeliveries: 2, // defaults to 2
        });

        consumer.setContext({ key: "value" });
        consumer.on('message', (message: Message, context: object) => {
            console.log(message.getData().toString());
            message.ack();
        });

        consumer.on('error', (error) => {
            console.log(error);
        });
    } catch (ex) {
        console.log(ex);
        if (memphisConnection) memphisConnection.close();
    }
})();
        