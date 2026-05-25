const { StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } = require("@aws-sdk/client-athena");
const { env } = require("../config/env");
const { state } = require("../state/runtimeState");
const { ValidationError, sendErrorResponse } = require("../lib/errors");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const runAthenaQuery = async (athenaClient, queryStr) => {
  const startCommand = new StartQueryExecutionCommand({
    QueryString: queryStr,
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

  return parsedData;
};

const getOverallMetrics = async (req, res) => {
  try {
    const athenaClient = state.api.athena;
    if (!athenaClient) {
      throw new Error("Athena client is not initialized");
    }

    const { date } = req.query;
    if (!date) {
      throw new ValidationError("Date parameter is required.");
    }

    const match = date.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) {
      throw new ValidationError("Invalid date format. Expected YYYY-MM-DD.");
    }
    const year = match[1];
    const month = match[2];
    const day = match[3];

    const cacheKey = `analytics:overall-metrics:${date}`;
    const cacheTTL = 10800; // 3 hours in seconds
    const redisClient = state.api.redis ? state.api.redis.client : null;

    if (redisClient) {
      try {
        const cachedData = await redisClient.get(cacheKey);
        if (cachedData) {
          state.api.logger?.info(`[getOverallMetrics] Cache hit for key: ${cacheKey}`);
          try {
            const parsedData = JSON.parse(cachedData);
            return res.status(200).json({
              message: "Overall metrics fetched successfully",
              data: parsedData,
            });
          } catch (parseError) {
            state.api.logger?.error(`[getOverallMetrics] Failed to parse cached JSON for key ${cacheKey}`, parseError);
          }
        } else {
          state.api.logger?.info(`[getOverallMetrics] Cache miss for key: ${cacheKey}`);
        }
      } catch (redisError) {
        state.api.logger?.error(`[getOverallMetrics] Redis error on GET for key ${cacheKey}`, redisError);
      }
    }

    const queryDaily = `
      SELECT
        SUM(total_tasks_scheduled) AS total_tasks_scheduled,
        SUM(total_tasks_completed) AS total_tasks_completed,
        SUM(total_tasks_failed) AS total_tasks_failed,
        ROUND(SUM(total_battery_consumed) / NULLIF(SUM(total_tasks_completed), 0), 4) AS avg_battery_consumed_per_task,
        SUM(total_task_duration_mins) AS total_task_duration_mins,
        SUM(idle_time_mins) AS idle_time_mins
      FROM
        ${env.athena.database}.fact_task_metrics_daily
      WHERE
        year  = '${year}'
        AND month = '${month}'
        AND day   = '${day}'
    `;

    const queryHourly = `
      SELECT
        interval_label,
        SUM(total_tasks_completed) AS completed,
        SUM(total_tasks_scheduled) AS scheduled,
        SUM(total_tasks_failed) AS failed
      FROM
        ${env.athena.database}.fact_task_metrics_2hr
      WHERE
        year  = '${year}'
        AND month = '${month}'
        AND day   = '${day}'
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

    const queryDurations = `
      SELECT
        COUNT(CASE WHEN task_duration_minutes >= 0 AND task_duration_minutes < 5 THEN 1 END) AS duration_0_5,
        COUNT(CASE WHEN task_duration_minutes >= 5 AND task_duration_minutes < 10 THEN 1 END) AS duration_5_10,
        COUNT(CASE WHEN task_duration_minutes >= 10 AND task_duration_minutes < 15 THEN 1 END) AS duration_10_15,
        COUNT(CASE WHEN task_duration_minutes >= 15 AND task_duration_minutes < 20 THEN 1 END) AS duration_15_20,
        COUNT(CASE WHEN task_duration_minutes >= 20 THEN 1 END) AS duration_20_plus
      FROM
        ${env.athena.database}.all_tasks_silver
      WHERE
        year  = '${year}'
        AND month = '${month}'
        AND day   = '${day}'
    `;

    const queryRobots = `
      SELECT
        robotid,
        COALESCE(SUM(total_tasks_completed), 0) AS total_tasks_completed,
        COALESCE(MAX(max_speed), 0.0) AS max_speed,
        COALESCE(SUM(total_task_duration_mins), 0.0) AS total_task_duration_mins
      FROM
        ${env.athena.database}.fact_task_metrics_daily
      WHERE
        year  = '${year}'
        AND month = '${month}'
        AND day   = '${day}'
      GROUP BY
        robotid
    `;

    const [dailyTotals, tasksThroughput, taskDurationsResult, robotMetricsResult] = await Promise.all([
      runAthenaQuery(athenaClient, queryDaily),
      runAthenaQuery(athenaClient, queryHourly),
      runAthenaQuery(athenaClient, queryDurations),
      runAthenaQuery(athenaClient, queryRobots)
    ]);

    const dailyTotalsRow = dailyTotals[0] || {};
    const totalTaskDurationMins = dailyTotalsRow.total_task_duration_mins || 0;
    const idleTimeMins = dailyTotalsRow.idle_time_mins || 0;
    const totalTime = totalTaskDurationMins + idleTimeMins;

    const fleetUtilization = {
      task_duration_percentage: totalTime > 0 ? Number(((totalTaskDurationMins / totalTime) * 100).toFixed(2)) : 0,
      idle_percentage: totalTime > 0 ? Number(((idleTimeMins / totalTime) * 100).toFixed(2)) : 0
    };

    const cleanDailyTotals = Object.keys(dailyTotalsRow).length > 0 ? {
      total_tasks_scheduled: dailyTotalsRow.total_tasks_scheduled || 0,
      total_tasks_completed: dailyTotalsRow.total_tasks_completed || 0,
      total_tasks_failed: dailyTotalsRow.total_tasks_failed || 0,
      avg_battery_consumed_per_task: dailyTotalsRow.avg_battery_consumed_per_task || 0
    } : null;

    const taskDurationsRow = taskDurationsResult[0] || {};
    const taskDurations = {
      duration_0_5: taskDurationsRow.duration_0_5 || 0,
      duration_5_10: taskDurationsRow.duration_5_10 || 0,
      duration_10_15: taskDurationsRow.duration_10_15 || 0,
      duration_15_20: taskDurationsRow.duration_15_20 || 0,
      duration_20_plus: taskDurationsRow.duration_20_plus || 0
    };

    const maxCompleted = Math.max(...robotMetricsResult.map(r => r.total_tasks_completed), 0);
    const maxSpeed = Math.max(...robotMetricsResult.map(r => r.max_speed), 0);
    const maxDuration = Math.max(...robotMetricsResult.map(r => r.total_task_duration_mins), 0);

    const scoredRobots = robotMetricsResult.map(robot => {
      const completedRatio = maxCompleted > 0 ? robot.total_tasks_completed / maxCompleted : 0;
      const speedRatio = maxSpeed > 0 ? robot.max_speed / maxSpeed : 0;
      const durationRatio = maxDuration > 0 ? robot.total_task_duration_mins / maxDuration : 0;

      // Weights: 50% tasks completed, 30% total duration, 20% max speed
      const score = (0.5 * completedRatio + 0.3 * durationRatio + 0.2 * speedRatio) * 100;

      return {
        robotid: robot.robotid,
        total_tasks_completed: robot.total_tasks_completed,
        max_speed: robot.max_speed,
        total_task_duration_mins: robot.total_task_duration_mins,
        score: Number(score.toFixed(2))
      };
    });

    const robotRankings = scoredRobots
      .sort((a, b) => b.score - a.score)
      .map((item, index) => ({
        ...item,
        rank: index + 1
      }));

    const requiredIntervals = [
      "08:00-10:00",
      "10:00-12:00",
      "12:00-14:00",
      "14:00-16:00",
      "16:00-18:00",
      "18:00-20:00"
    ];

    const cleanTasksThroughput = requiredIntervals.map(label => {
      const foundRow = tasksThroughput.find(row => row.interval_label === label);
      return {
        interval_label: label,
        completed: foundRow ? (foundRow.completed || 0) : 0,
        scheduled: foundRow ? (foundRow.scheduled || 0) : 0,
        failed: foundRow ? (foundRow.failed || 0) : 0
      };
    });

    const responseData = {
      dailyTotals: cleanDailyTotals,
      fleetUtilization,
      taskDurations,
      tasksThroughput: cleanTasksThroughput,
      robotRankings
    };

    if (redisClient) {
      try {
        await redisClient.set(cacheKey, JSON.stringify(responseData), "EX", cacheTTL);
        state.api.logger?.info(`[getOverallMetrics] Cached overall metrics for key: ${cacheKey} with TTL: ${cacheTTL}`);
      } catch (redisError) {
        state.api.logger?.error(`[getOverallMetrics] Redis error on SET for key ${cacheKey}`, redisError);
      }
    }

    return res.status(200).json({
      message: "Overall metrics fetched successfully",
      data: responseData,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const getRobotMetrics = async (req, res) => {
  try {
    const athenaClient = state.api.athena;
    if (!athenaClient) {
      throw new Error("Athena client is not initialized");
    }

    const { robotId } = req.params;
    const { date } = req.query;

    if (!robotId) {
      throw new ValidationError("robotId parameter is required.");
    }

    if (!robotId.match(/^[a-zA-Z0-9_-]+$/)) {
      throw new ValidationError("Invalid robotId format.");
    }
    
    if (!date) {
      throw new ValidationError("Date parameter is required.");
    }

    const match = date.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) {
      throw new ValidationError("Invalid date format. Expected YYYY-MM-DD.");
    }
    const year = match[1];
    const month = match[2];
    const day = match[3];

    const cacheKey = `analytics:robot-metrics:${robotId}:${date}`;
    const cacheTTL = 10800; // 3 hours in seconds
    const redisClient = state.api.redis ? state.api.redis.client : null;

    if (redisClient) {
      try {
        const cachedData = await redisClient.get(cacheKey);
        if (cachedData) {
          state.api.logger?.info(`[getRobotMetrics] Cache hit for key: ${cacheKey}`);
          try {
            const parsedData = JSON.parse(cachedData);
            return res.status(200).json({
              message: "Robot metrics fetched successfully",
              data: parsedData,
            });
          } catch (parseError) {
            state.api.logger?.error(`[getRobotMetrics] Failed to parse cached JSON for key ${cacheKey}`, parseError);
          }
        } else {
          state.api.logger?.info(`[getRobotMetrics] Cache miss for key: ${cacheKey}`);
        }
      } catch (redisError) {
        state.api.logger?.error(`[getRobotMetrics] Redis error on GET for key ${cacheKey}`, redisError);
      }
    }

    const queryRobotStats = `
      SELECT
        SUM(total_tasks_scheduled) AS total_tasks_scheduled,
        SUM(total_tasks_completed) AS total_tasks_completed,
        ROUND(SUM(total_battery_consumed) / NULLIF(SUM(total_tasks_completed), 0), 4) AS avg_battery_per_task,
        ROUND((CAST(SUM(total_tasks_completed) AS double) * 100.0) / NULLIF(SUM(total_tasks_scheduled), 0), 4) AS success_rate_percent,
        ROUND(AVG(avg_speed), 4) AS avg_speed_per_task,
        SUM(total_task_duration_mins) AS total_task_duration_mins,
        ROUND(AVG(avg_tasks_per_charge_cycle), 4) AS avg_tasks_per_charge_cycle,
        SUM(total_charge_cycles) AS total_charge_cycles
      FROM
        ${env.athena.database}.fact_task_metrics_daily
      WHERE
        robotid = '${robotId}'
        AND year = '${year}'
        AND month = '${month}'
        AND day = '${day}'
    `;

    const queryIntervalMetrics = `
      SELECT
        interval_label,
        ROUND(SUM(total_battery_consumed), 4) AS total_battery_consumed,
        SUM(total_tasks_completed) AS total_tasks_completed,
        SUM(total_tasks_failed) AS total_tasks_failed
      FROM
        ${env.athena.database}.fact_task_metrics_2hr
      WHERE
        robotid = '${robotId}'
        AND year = '${year}'
        AND month = '${month}'
        AND day = '${day}'
      GROUP BY
        interval_label
      ORDER BY
        interval_label
    `;

    const [robotStatsResult, intervalMetricsResult] = await Promise.all([
      runAthenaQuery(athenaClient, queryRobotStats),
      runAthenaQuery(athenaClient, queryIntervalMetrics)
    ]);

    const robotStatsRow = robotStatsResult[0] || {};
    
    const shiftMinutes = (env.analytics.shiftDurationHours || 8) * 60;
    const totalTaskDurationMins = robotStatsRow.total_task_duration_mins || 0;
    const idleTimeMins = Math.max(0, shiftMinutes - totalTaskDurationMins);
    
    const utilization = {
      task_duration_percentage: Number(((totalTaskDurationMins / shiftMinutes) * 100).toFixed(2)),
      idle_percentage: Number(((idleTimeMins / shiftMinutes) * 100).toFixed(2))
    };

    const requiredIntervals = [
      "08:00-10:00",
      "10:00-12:00",
      "12:00-14:00",
      "14:00-16:00",
      "16:00-18:00",
      "18:00-20:00"
    ];

    const batteryIntervals = requiredIntervals.map(label => {
      const foundRow = intervalMetricsResult.find(row => row.interval_label === label);
      return {
        interval_label: label,
        total_battery_consumed: foundRow ? (foundRow.total_battery_consumed || 0) : 0
      };
    });

    const tasksThroughput = requiredIntervals.map(label => {
      const foundRow = intervalMetricsResult.find(row => row.interval_label === label);
      return {
        interval_label: label,
        completed: foundRow ? (foundRow.total_tasks_completed || 0) : 0,
        failed: foundRow ? (foundRow.total_tasks_failed || 0) : 0
      };
    });

    const responseData = {
      robotId,
      date,
      metrics: {
        total_tasks_scheduled: robotStatsRow.total_tasks_scheduled || 0,
        total_tasks_completed: robotStatsRow.total_tasks_completed || 0,
        avg_battery_per_task: robotStatsRow.avg_battery_per_task || 0,
        success_rate_percent: robotStatsRow.success_rate_percent || 0,
        avg_speed_per_task: robotStatsRow.avg_speed_per_task || 0,
        avg_tasks_per_charge_cycle: robotStatsRow.avg_tasks_per_charge_cycle || 0,
        total_charge_cycles: robotStatsRow.total_charge_cycles || 0
      },
      utilization,
      batteryIntervals,
      tasksThroughput
    };

    if (redisClient) {
      try {
        await redisClient.set(cacheKey, JSON.stringify(responseData), "EX", cacheTTL);
        state.api.logger?.info(`[getRobotMetrics] Cached robot metrics for key: ${cacheKey} with TTL: ${cacheTTL}`);
      } catch (redisError) {
        state.api.logger?.error(`[getRobotMetrics] Redis error on SET for key ${cacheKey}`, redisError);
      }
    }

    return res.status(200).json({
      message: "Robot metrics fetched successfully",
      data: responseData,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

module.exports = {
  getOverallMetrics,
  getRobotMetrics,
};


