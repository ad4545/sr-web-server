const MAX_SPEED_SAMPLES = 10000;

const roundSpeedSample = (value) => {
  return Number(value.toFixed(2));
};

class TaskSession {
  constructor({
    taskID,
    originalTaskID,
    robotID,
    batteryStart,
    timeStart,
    setIntervalFn = setInterval,
    clearIntervalFn = clearInterval,
    getLatestSpeed,
  }) {
    this.taskID = taskID;
    this.originalTaskID = originalTaskID;
    this.robotID = robotID;
    this.status = "Active";
    this.batteryStart = batteryStart;
    this.timeStart = timeStart;
    this.createdAt = Date.now();
    this.speedSamples = [];
    
    this.setIntervalFn = setIntervalFn;
    this.clearIntervalFn = clearIntervalFn;
    this.getLatestSpeed = getLatestSpeed;
    
    this.sampleTimer = null;
  }

  startSampling(intervalMs = 2000) {
    if (this.sampleTimer) {
      return;
    }

    this.sampleTimer = this.setIntervalFn(() => {
      const currentSpeed = this.getLatestSpeed();
      if (!currentSpeed || typeof currentSpeed.linearX !== "number" || Number.isNaN(currentSpeed.linearX)) {
        return;
      }

      // Store the absolute value to ensure positivity
      const positiveSpeed = Math.abs(currentSpeed.linearX);
      if (this.speedSamples.length < MAX_SPEED_SAMPLES) {
        this.speedSamples.push(roundSpeedSample(positiveSpeed));
      }
    }, intervalMs);
  }

  stopSampling() {
    if (this.sampleTimer) {
      this.clearIntervalFn(this.sampleTimer);
      this.sampleTimer = null;
    }
  }
}

module.exports = {
  MAX_SPEED_SAMPLES,
  TaskSession,
  roundSpeedSample,
};
