const crypto = require("crypto");
const { TaskSession, roundSpeedSample } = require("./TaskSession");
const { uploadTaskCompletionRecord, buildTaskCompletionKey } = require("./uploader");
const {
  TASK_COMPLETION_PARQUET_SCHEMA,
  buildTaskCompletionRecord,
  buildTaskCompletionRow,
  writeTaskCompletionParquet,
} = require("./parquet");

const SESSION_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
const SESSION_SWEEP_INTERVAL_MS = 60 * 1000; // check every 60 seconds
const UPLOAD_MAX_RETRIES = 3;
const UPLOAD_BASE_DELAY_MS = 1000;

const normalizeTaskStatus = (status) => {
  const normalized = typeof status === "string" ? status.trim() : "";

  if (normalized === "Active") return "Active";
  if (normalized === "Success") return "Success";
  if (normalized === "Failed") return "Failed";

  return "";
};

class TaskCompletionTracker {
  constructor({
    s3,
    bucketName,
    logger,
    now = () => new Date().toISOString(),
    setIntervalFn = setInterval,
    clearIntervalFn = clearInterval,
    uploadRecord = uploadTaskCompletionRecord,
  }) {
    if (!bucketName) {
      throw new Error("TASK_COMPLETION_S3_BUCKET is required");
    }

    this.s3 = s3;
    this.bucketName = bucketName;
    this.logger = logger;
    this.now = now;
    this.setIntervalFn = setIntervalFn;
    this.clearIntervalFn = clearIntervalFn;
    this.uploadRecord = uploadRecord;

    this.latestBatteryByRobot = new Map();
    this.latestSpeedByRobot = new Map();
    this.activeTaskByRobot = new Map();
    this.pendingUploads = new Set();
    this.closed = false;

    // Periodic sweep to clean up stale sessions (e.g. robot crashed mid-task)
    this.sessionSweepTimer = this.setIntervalFn(() => this.sweepStaleSessions(), SESSION_SWEEP_INTERVAL_MS);
  }

  updateBattery({ robotID, soc }) {
    if (this.closed || typeof robotID !== "string" || typeof soc !== "number" || Number.isNaN(soc)) {
      return;
    }
    this.latestBatteryByRobot.set(robotID, { soc, seenAt: this.now() });
  }

  updateSpeed({ robotID, linearX }) {
    if (this.closed) return;
    if (typeof robotID !== "string") return;
    if (typeof linearX !== "number" || Number.isNaN(linearX)) {
      this.logger.warn("updateSpeed skipped: linearX is not a valid number", {
        robotID,
        linearX,
        type: typeof linearX,
      });
      return;
    }
    this.latestSpeedByRobot.set(robotID, { linearX, seenAt: this.now() });
  }

  startTask({ taskID, robotID }) {
    const existingSession = this.activeTaskByRobot.get(robotID);

    if (existingSession) {
      if (existingSession.originalTaskID === taskID) {
        // Ignore duplicate Active signals for the currently running task
        return;
      }

      this.logger.warn("Replacing active task completion session for robot", {
        robotID,
        previousSessionID: existingSession.taskID,
      });
      existingSession.stopSampling();
      this.activeTaskByRobot.delete(robotID);
    }

    const currentBattery = this.latestBatteryByRobot.get(robotID);
    if (!currentBattery) {
      this.logger.warn(
        "Battery not yet available at task start; session will proceed with batteryStart=null",
        { robotID }
      );
    }

    // Generate a unique ID for the session to separate it entirely from the gRPC taskID
    const sessionID = crypto.randomUUID();

    const session = new TaskSession({
      taskID: sessionID,
      originalTaskID: taskID,
      robotID,
      batteryStart: currentBattery ? currentBattery.soc : null,
      timeStart: this.now(),
      setIntervalFn: this.setIntervalFn,
      clearIntervalFn: this.clearIntervalFn,
      getLatestSpeed: () => this.latestSpeedByRobot.get(robotID),
    });

    session.startSampling(2000);
    this.activeTaskByRobot.set(robotID, session);
  }

  completeTask({ robotID, status }) {
    const session = this.activeTaskByRobot.get(robotID);
    if (!session) {
      return;
    }

    const currentBattery = this.latestBatteryByRobot.get(robotID);
    session.stopSampling();
    this.activeTaskByRobot.delete(robotID);

    // Strict validation: Do not store unless all required data is filled
    if (
      !session.batteryStart ||
      !currentBattery ||
      !session.timeStart ||
      session.speedSamples.length === 0
    ) {
      this.logger.warn("Skipping task completion upload due to missing or incomplete data", {
        sessionID: session.taskID,
        robotID,
        status,
        hasBatteryStart: !!session.batteryStart,
        hasBatteryEnd: !!currentBattery,
        speedSampleCount: session.speedSamples.length,
      });
      return;
    }

    const record = buildTaskCompletionRecord({
      taskID: session.taskID,
      status,
      robotID,
      speed: session.speedSamples,
      batteryStart: session.batteryStart,
      batteryEnd: currentBattery.soc,
      timeStart: session.timeStart,
      timeEnd: this.now(),
    });
    this.createUploadTask(record);
  }

  handleTaskFeedback({ taskID, status, robotID }) {
    if (this.closed) return;

    const normalizedStatus = normalizeTaskStatus(status);
    if (normalizedStatus === "Active") {
      this.startTask({ taskID, robotID });
      return;
    }

    if (normalizedStatus === "Success" || normalizedStatus === "Failed") {
      this.completeTask({ robotID, status: normalizedStatus });
    }
  }

  createUploadTask(record) {
    const uploadPromise = this.uploadWithRetry(record)
      .catch((error) => {
        this.logger.error("Task completion upload failed after all retries", {
          sessionID: record.taskID,
          robotID: record.robotID,
          error: error.message,
        });
      })
      .finally(() => {
        this.pendingUploads.delete(uploadPromise);
      });

    this.pendingUploads.add(uploadPromise);
  }

  async uploadWithRetry(record) {
    let lastError;

    for (let attempt = 1; attempt <= UPLOAD_MAX_RETRIES; attempt++) {
      try {
        return await this.uploadRecord({
          s3: this.s3,
          bucketName: this.bucketName,
          record,
        });
      } catch (error) {
        lastError = error;

        if (attempt < UPLOAD_MAX_RETRIES) {
          const delayMs = UPLOAD_BASE_DELAY_MS * Math.pow(2, attempt - 1);
          this.logger.warn("Task completion upload attempt failed, retrying", {
            sessionID: record.taskID,
            robotID: record.robotID,
            attempt,
            nextRetryMs: delayMs,
            error: error.message,
          });
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
      }
    }

    throw lastError;
  }

  sweepStaleSessions() {
    const now = Date.now();

    for (const [robotID, session] of this.activeTaskByRobot.entries()) {
      const elapsed = now - session.createdAt;

      if (elapsed > SESSION_TIMEOUT_MS) {
        this.logger.warn("Auto-closing stale task session (timeout exceeded)", {
          robotID,
          sessionID: session.taskID,
          elapsedMs: elapsed,
          timeoutMs: SESSION_TIMEOUT_MS,
        });
        session.stopSampling();
        this.activeTaskByRobot.delete(robotID);
      }
    }
  }

  async close() {
    if (this.closed) return;
    this.closed = true;

    if (this.sessionSweepTimer) {
      this.clearIntervalFn(this.sessionSweepTimer);
      this.sessionSweepTimer = null;
    }

    for (const session of this.activeTaskByRobot.values()) {
      session.stopSampling();
    }
    this.activeTaskByRobot.clear();

    await Promise.allSettled([...this.pendingUploads]);
  }

  getState() {
    return {
      latestBatteryByRobot: Object.fromEntries(this.latestBatteryByRobot),
      latestSpeedByRobot: Object.fromEntries(this.latestSpeedByRobot),
      activeTaskByRobot: Object.fromEntries(
        [...this.activeTaskByRobot.entries()].map(([robotID, session]) => [
          robotID,
          {
            sessionID: session.taskID,
            robotID: session.robotID,
            status: session.status,
            batteryStart: session.batteryStart,
            timeStart: session.timeStart,
            speedSamples: [...session.speedSamples],
          },
        ])
      ),
      pendingUploads: this.pendingUploads.size,
    };
  }

  deleteBatterySnapshot(robotID) {
    this.latestBatteryByRobot.delete(robotID);
  }
}

const createTaskCompletionTracker = (options) => {
  const tracker = new TaskCompletionTracker(options);
  return {
    updateBattery: tracker.updateBattery.bind(tracker),
    updateSpeed: tracker.updateSpeed.bind(tracker),
    handleTaskFeedback: tracker.handleTaskFeedback.bind(tracker),
    close: tracker.close.bind(tracker),
    getState: tracker.getState.bind(tracker),
    deleteBatterySnapshot: tracker.deleteBatterySnapshot.bind(tracker),
  };
};

module.exports = {
  TASK_COMPLETION_PARQUET_SCHEMA,
  buildTaskCompletionKey,
  buildTaskCompletionRecord,
  buildTaskCompletionRow,
  createTaskCompletionTracker,
  normalizeTaskStatus,
  roundSpeedSample,
  uploadTaskCompletionRecord,
  writeTaskCompletionParquet,
};
