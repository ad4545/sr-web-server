const test = require("node:test");
const assert = require("node:assert/strict");

const { NotFoundError } = require("../../lib/errors");
const { createTasksHandler } = require("../../api/handlers/tasks");

test("tasks handler paginates results with existing page contract", async () => {
  const tasksHandler = createTasksHandler({
    collection: {
      find() {
        return {
          skip() {
            return {
              limit() {
                return {
                  async toArray() {
                    return [{ masterTaskName: "task-2" }];
                  },
                };
              },
            };
          },
        };
      },
      async countDocuments() {
        return 7;
      },
    },
    kafkaProducer: { async send() {} },
  });

  const res = {
    status(code) {
      assert.equal(code, 200);
      return this;
    },
    json(data) {
      assert.deepEqual(data, {
        message: "Tasks fetched successfully",
        currentPage: 2,
        totalPages: 3,
        totalCount: 7,
        hasNextPage: true,
        hasPrevPage: true,
        data: [{ masterTaskName: "task-2" }],
      });
    },
  };

  await tasksHandler.getTasks({ query: { page: "2" } }, res);
});

test("tasks handler publishes validated task command through transport port", async () => {
  const published = [];
  const tasksHandler = createTasksHandler({
    collection: {},
    kafkaProducer: {
      async send(message) {
        published.push(message);
      },
    },
  });

  const res = {
    status(code) {
      assert.equal(code, 200);
      return this;
    },
    json(data) {
      assert.ok(data.message.includes("published"));
    },
  };

  await tasksHandler.sendTask(
    {
      body: {
        task: {
          masterTaskName: "master-task",
          topic: "robot.tasks",
          tasks: [
            {
              taskName: "deliver",
              type: "move",
            },
          ],
        },
      },
    },
    res
  );

  assert.deepEqual(published, [
    {
      topic: "robot.tasks",
      messages: [
        {
          key: "master-task",
          value: JSON.stringify({
            masterTaskName: "master-task",
            tasks: [
              {
                taskName: "deliver",
                type: "move",
              },
            ],
          }),
        },
      ],
    },
  ]);
});

test("tasks handler throws not found when delete target does not exist", async () => {
  const tasksHandler = createTasksHandler({
    collection: {
      async deleteOne() {
        return { deletedCount: 0 };
      },
    },
    kafkaProducer: { async send() {} },
  });

  await assert.rejects(
    () => tasksHandler.deleteTask({ params: { title: "missing" } }, {}),
    NotFoundError
  );
});
