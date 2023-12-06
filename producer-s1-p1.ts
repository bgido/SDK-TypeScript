import * as dotenv from 'dotenv';
dotenv.config();

// Access environment variables
const AccountID: number = parseInt(process.env.AccountID || '223671990', 10);
const Host: string = process.env.Host || '';
const User: string = process.env.User || '';
const Pass: string = process.env.Pass || '';

import { memphis, Memphis } from 'memphis-dev';
// Function to create a delay using a Promise
function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
(async function () {
    let memphisConnection: Memphis | null = null;

    try {
        memphisConnection = await memphis.connect({
            host: Host,
            username: User,
            password: Pass,
            accountId: AccountID
        });

        const producer = await memphisConnection.producer({
            stationName: 'eventlog-1',
            producerName: 'producer-TS-Barak-1'
        });
            
		const headers = memphis.headers()
        headers.add('key', 'value');
        const msg = {
            fname: "Barak",
			lname: "Gido",
			age: "33",
			city: "Tel Aviv",
        }
        let counter = 0;

        while (true) {
            console.log(`Iteration ${counter}`);
            counter++;
            if (counter % 5000 === 0) {
                console.log('Taking a break for 5 seconds...');
                // Add a delay of 5 seconds (5000 milliseconds)
                await delay(5000);
                console.log('Resuming...');
            }
            await producer.produce({
                message: msg,
                headers: headers,
                asyncProduce: true
            });
    }
 } catch (ex) {
        console.log(ex);
        if (memphisConnection) memphisConnection.close();
    }
})();
        