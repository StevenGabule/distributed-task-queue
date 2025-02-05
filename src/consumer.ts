import {Kafka, logLevel, Partitioners} from 'kafkajs';
import {Task} from './task';

const kafka = new Kafka({
	clientId: 'task-consumer',
	brokers: ['localhost:9092'],
	logLevel: logLevel.INFO
});

const consumer = kafka.consumer({groupId: 'task-worker-group'});
const producer = kafka.producer({createPartitioner: Partitioners.LegacyPartitioner})
const MAX_RETRIES = 3;

async function processTask(task: Task): Promise<boolean> {
  console.log(`Processing task ${task.id} with order ${task.payload.orderId}`);
	const success = Math.random() > 0.3;
	return success;
}

async function run() {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'tasks', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const task: Task = JSON.parse(message.value!.toString());
      console.log(`Received task ${task.id} (retry count: ${task.retryCount})`);
      
      const success = await processTask(task);
      if (success) {
        console.log(`Task ${task.id} processed successfully`);
      } else {
        // If task fails, increment retry count
        task.retryCount = (task.retryCount || 0) + 1;

        if (task.retryCount <= MAX_RETRIES) {
          // Calculate exponential backoff delay (in ms)
          const delay = Math.pow(2, task.retryCount) * 1000;
          console.log(`Task ${task.id} failed. Retrying in ${delay} ms`);
        
          // Simple delay (in a real system, you might use a scheduling service)
          setTimeout(async () => {
            await producer.send({
              topic: 'tasks',
              messages: [{ 
                key: task.priority.toString(), 
                value: JSON.stringify(task) 
              }]
            });
            console.log(`Requeued task ${task.id} for retry ${task.retryCount}`);
          }, delay);
        } else {
          // Send task to dead-letter queue after exceeding retries
          console.log(`Task ${task.id} exceeded max retries. Sending to DLQ.`);
          await producer.send({
            topic: 'tasks.DLQ',
            messages: [{ key: task.priority.toString(), value: JSON.stringify(task) }]
          });
        }
      }
    }
  });
}

run().catch(console.error);