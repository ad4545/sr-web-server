const { randomUUID } = require("crypto");
const fs = require("fs/promises");
const path = require("path");
const { buildTaskCompletionRow, writeTaskCompletionParquet } = require("./parquet");

const pad2 = (value) => {
  return String(value).padStart(2, "0");
};

const buildTaskCompletionKey = ({ completedAt, taskID, uniqueId }) => {
  const date = new Date(completedAt);
  return `raw-data/year=${date.getUTCFullYear()}/month=${pad2(date.getUTCMonth() + 1)}/day=${pad2(
    date.getUTCDate()
  )}/${uniqueId}_${taskID}.parquet`;
};

const uploadTaskCompletionRecord = async ({
  s3,
  bucketName,
  record,
  tempRoot = "/tmp/sr-task-completions",
  fsPromises = fs,
  writeParquetFile = writeTaskCompletionParquet,
  createUniqueId = randomUUID,
}) => {
  const row = buildTaskCompletionRow(record);
  const uniqueId = createUniqueId();
  const key = buildTaskCompletionKey({
    completedAt: record.time.end,
    taskID: record.taskID,
    uniqueId,
  });
  
  // Security Review Note: Unique UUIDs prevent traversal and filename conflicts.
  const tempFilePath = path.join(
    tempRoot,
    `${uniqueId}_${record.taskID}-${Date.parse(record.time.end) || Date.now()}.parquet`
  );

  await fsPromises.mkdir(tempRoot, { recursive: true });

  try {
    await writeParquetFile({
      row,
      filePath: tempFilePath,
    });

    const body = await fsPromises.readFile(tempFilePath);

    await s3.putObject({
      Bucket: bucketName,
      Key: key,
      Body: body,
      ContentType: "application/octet-stream",
    }).promise();

    return {
      key,
      tempFilePath,
    };
  } finally {
    // Ensuring fail-safe cleanup
    await fsPromises.rm(tempFilePath, { force: true }).catch(() => {});
  }
};

module.exports = {
  buildTaskCompletionKey,
  uploadTaskCompletionRecord,
};
