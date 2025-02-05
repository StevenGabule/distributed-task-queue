// Build the Producer Service
// Produce tasks to a Kafka topic named "tasks".
import { Kafka, logLevel, Partitioners } from 'kafkajs';
import {v4 as uuidv4} from 'uuid';
import {Task} from './task'

// Configure KafkaJS client
const kafka = new Kafka({
	clientId: 'task-producer',
	brokers: ['localhost:9092'],
	logLevel: logLevel.INFO
})

const producer = kafka.producer({
	createPartitioner: Partitioners.LegacyPartitioner
});

async function produceTask(task: Task) {
	await producer.send({
		topic: 'tasks',
		messages: [
			{
				key: task.priority.toString(),
				value: JSON.stringify(task)
			}
		]
	});
	console.log(`Task ${task.id} produced.`)
}

async function run() {
	await producer.connect();

	// Simulate creating tasks
	for(let i = 1; i <= 10; i++) {
		const task: Task = {
			id: uuidv4(),
			payload: { orderId: `order-${i}`, details: `Process order ${i}`},
			priority: Math.floor(Math.random() * 10), // random priority
			retryCount: 0
		};
		await produceTask(task);
	}

	await producer.disconnect();
}

run().catch(console.error);













