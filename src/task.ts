export interface Task {
	id: string;
	payload: any; 
	priority: number;
	retryCount?: number;
}