const { ParquetSchema, ParquetWriter } = require("parquetjs-lite");

const TASK_COMPLETION_PARQUET_SCHEMA = new ParquetSchema({
  taskID: { type: "UTF8" },
  status: { type: "UTF8" },
  robotID: { type: "UTF8" },
  speed: { type: "DOUBLE", repeated: true },
  battery_start: { type: "DOUBLE", optional: true },
  battery_end: { type: "DOUBLE", optional: true },
  time_start: { type: "UTF8" },
  time_end: { type: "UTF8" },
});

const buildTaskCompletionRecord = ({
  taskID,
  status,
  robotID,
  speed,
  batteryStart,
  batteryEnd,
  timeStart,
  timeEnd,
}) => {
  return {
    taskID,
    status,
    robotID,
    speed: Array.isArray(speed) ? [...speed] : [],
    battery: {
      start: batteryStart,
      end: batteryEnd,
    },
    time: {
      start: timeStart,
      end: timeEnd,
    },
  };
};

const buildTaskCompletionRow = (record) => {
  return {
    taskID: record.taskID,
    status: record.status,
    robotID: record.robotID,
    speed: Array.isArray(record.speed) ? [...record.speed] : [],
    battery_start: record.battery.start ?? null,
    battery_end: record.battery.end ?? null,
    time_start: record.time.start,
    time_end: record.time.end,
  };
};

const writeTaskCompletionParquet = async ({ row, filePath }) => {
  const writer = await ParquetWriter.openFile(TASK_COMPLETION_PARQUET_SCHEMA, filePath);

  try {
    await writer.appendRow(row);
  } finally {
    await writer.close();
  }
};

module.exports = {
  TASK_COMPLETION_PARQUET_SCHEMA,
  buildTaskCompletionRecord,
  buildTaskCompletionRow,
  writeTaskCompletionParquet,
};
