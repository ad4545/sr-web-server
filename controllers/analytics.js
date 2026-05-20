const { StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } = require("@aws-sdk/client-athena");
const { env } = require("../config/env");
const { state } = require("../state/runtimeState");
const { ValidationError, sendErrorResponse } = require("../lib/errors");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const getOverallMetrics = async (req, res) => {
  try {
    const athenaClient = state.api.athena;
    if (!athenaClient) {
      throw new Error("Athena client is not initialized");
    }

    const { date } = req.query;
    let year = 2026;
    let month = 5;
    let day = 16;

    if (date) {
      const match = date.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (!match) {
        throw new ValidationError("Invalid date format. Expected YYYY-MM-DD.");
      }
      year = parseInt(match[1], 10);
      month = parseInt(match[2], 10);
      day = parseInt(match[3], 10);
    }

    const query = `
      SELECT
        interval_label,
        SUM(total_tasks_scheduled)                                        AS total_tasks_scheduled,
        SUM(total_tasks_completed)                                        AS total_tasks_completed,
        SUM(total_tasks_failed)                                           AS total_tasks_failed,
        ROUND(SUM(total_tasks_completed) * 1.0 / SUM(total_tasks_scheduled), 4) AS task_completion_rate,
        ROUND(AVG(avg_speed), 2)                                          AS avg_speed_per_task,
        ROUND(AVG(avg_task_duration_mins), 2)                             AS avg_time_per_task_mins,
        ROUND(SUM(total_battery_consumed), 2)                             AS total_battery_consumed
      FROM
        ${env.athena.database}.fact_task_metrics_2hr
      WHERE
        year  = ${year}
        AND month = ${month}
        AND day   = ${day}
        AND interval_label IN (
          '08:00-10:00',
          '10:00-12:00',
          '12:00-14:00',
          '14:00-16:00',
          '16:00-18:00',
          '18:00-20:00'
        )
      GROUP BY
        interval_label
      ORDER BY
        interval_label
    `;

    const startCommand = new StartQueryExecutionCommand({
      QueryString: query,
      QueryExecutionContext: {
        Database: env.athena.database,
      },
      ResultConfiguration: {
        OutputLocation: env.athena.resultsBucket,
      },
      WorkGroup: env.athena.workgroup,
    });

    const startResponse = await athenaClient.send(startCommand);
    const queryExecutionId = startResponse.QueryExecutionId;

    let queryState = "RUNNING";
    while (queryState === "RUNNING" || queryState === "QUEUED") {
      const getExecutionCommand = new GetQueryExecutionCommand({
        QueryExecutionId: queryExecutionId,
      });
      const getExecutionResponse = await athenaClient.send(getExecutionCommand);
      queryState = getExecutionResponse.QueryExecution.Status.State;

      if (queryState === "FAILED" || queryState === "CANCELLED") {
        throw new Error(`Athena query failed or was cancelled. State: ${queryState}`);
      }

      if (queryState === "SUCCEEDED") {
        break;
      }

      await sleep(1000);
    }

    const getResultsCommand = new GetQueryResultsCommand({
      QueryExecutionId: queryExecutionId,
    });
    const getResultsResponse = await athenaClient.send(getResultsCommand);

    const rows = getResultsResponse.ResultSet.Rows;
    const parsedData = [];

    if (rows && rows.length > 1) {
      const columnNames = rows[0].Data.map((col) => col.VarCharValue);

      for (let i = 1; i < rows.length; i++) {
        const rowData = {};
        rows[i].Data.forEach((col, index) => {
          const val = col.VarCharValue;
          const colName = columnNames[index];
          if (val !== undefined && !isNaN(Number(val)) && colName !== 'interval_label') {
            rowData[colName] = Number(val);
          } else {
            rowData[colName] = val;
          }
        });
        parsedData.push(rowData);
      }
    }

    return res.status(200).json({
      message: "Overall metrics fetched successfully",
      data: parsedData,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

module.exports = {
  getOverallMetrics,
};



