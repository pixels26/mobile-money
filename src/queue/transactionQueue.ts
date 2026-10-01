import { Queue } from "bullmq";
import { queueOptions } from "./config";
import { injectTraceContext } from "./jobTracing";
import { TransactionModel, TransactionStatus } from "../models/transaction";

export const TRANSACTION_QUEUE_NAME = "transaction-processing-queue";
export const TRANSACTION_JOB_NAME = "transaction-process";

const transactionModel = new TransactionModel();

export interface TransactionJobData {
  transactionId: string;
  type: "deposit" | "withdraw";
  amount: string;
  phoneNumber: string;
  provider: string;
  stellarAddress: string;
  /** IP address of the originating client, forwarded through the queue for blacklist enforcement. */
  clientIp?: string;
  requestId?: string;
  _traceId?: string;
  /** Serialized W3C `traceparent` injected at enqueue time (see queue/jobTracing). */
  _traceparent?: string;
}

export interface TransactionJobResult {
  success: boolean;
  transactionId: string;
  error?: string;
}

// Instantiate the BullMQ Queue. The old RabbitMQ work queue has been
// replaced by BullMQ so all transaction processing shares the same
// Redis-backed cluster primitives (retries, backoff, retention, DLQ)
// used by the other queues in this application.
export const transactionQueue = new Queue<
  TransactionJobData,
  TransactionJobResult
>(TRANSACTION_QUEUE_NAME, {
  ...queueOptions,
  defaultJobOptions: {
    ...queueOptions.defaultJobOptions,
    // Broker-level retries are intentionally conservative: the worker
    // performs its own in-process retries (withRetry) so failed jobs are
    // never re-executed from scratch (which could double-send a payment).
    attempts: 1,
    // Retention: clean up completed/failed job records to bound Redis memory
    removeOnComplete: {
      count: 1000,
      age: 24 * 3600, // 24 hours
    },
    removeOnFail: {
      count: 500,
      age: 7 * 24 * 3600, // 7 days
    },
  },
});

/**
 * Enqueue a transaction for processing.
 * Returns a job-shaped result so callers can surface a jobId.
 */
export async function addTransactionJob(
  data: TransactionJobData,
  options?: {
    priority?: number;
    delay?: number;
    repeat?: { every: number };
    jobId?: string;
  },
): Promise<{ id: string | undefined }> {
  // Embed the active OpenTelemetry context so the worker continues the same
  // distributed trace (HTTP → queue → processor).
  const job = await transactionQueue.add(
    TRANSACTION_JOB_NAME,
    injectTraceContext(data),
    {
      jobId: options?.jobId ?? data.transactionId,
      priority: options?.priority,
      delay: options?.delay,
      repeat: options?.repeat,
    },
  );

  return { id: job.id ?? data.transactionId };
}

/**
 * Fetch a job by ID, falling back to the transaction row when the job has
 * already been removed from Redis (retention window exceeded).
 */
export async function getJobById(jobId: string) {
  const job = await transactionQueue.getJob(jobId);
  if (job) {
    return job;
  }
  return await transactionModel.findById(jobId);
}

/**
 * Resolve processing progress from the BullMQ job, falling back to the
 * transaction status when the job is no longer retained.
 */
export async function getJobProgress(jobId: string): Promise<number> {
  const job = await transactionQueue.getJob(jobId);
  if (job) {
    const progress = job.progress;
    if (typeof progress === "number") return progress;
    return 0;
  }

  const transaction = await transactionModel.findById(jobId);
  if (!transaction) return 0;

  if (transaction.status === TransactionStatus.Completed) return 100;
  if (transaction.status === TransactionStatus.Failed) return 0;
  return 0;
}

/**
 * Get transaction queue health metrics.
 */
export async function getQueueStats() {
  const [waiting, active, completed, failed] = await Promise.all([
    transactionQueue.getWaitingCount(),
    transactionQueue.getActiveCount(),
    transactionQueue.getCompletedCount(),
    transactionQueue.getFailedCount(),
  ]);

  return {
    waiting,
    active,
    completed,
    failed,
    isPaused: await transactionQueue.isPaused(),
  };
}

export async function pauseQueue() {
  await transactionQueue.pause();
}

export async function resumeQueue() {
  await transactionQueue.resume();
}

export async function drainQueue() {
  await transactionQueue.drain();
}
