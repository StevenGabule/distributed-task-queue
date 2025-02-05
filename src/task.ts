// Define the structure of a task message
export interface Task {
	id: string;
	payload: any; // type this based on your domain (order, payment, etc.)
	priority: number;
	retryCount?: number;
}