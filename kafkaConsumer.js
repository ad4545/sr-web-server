const { Kafka } = require('kafkajs');

// ── 1. Kafka Client ────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'robot-feedback-consumer',
  brokers: ['10.0.0.12:9092'],
});

// ── 2. Consumers ───────────────────────────────────────────────────────────
const consumer     = kafka.consumer({ groupId: 'robot-feedback-group' });
const odomConsumer = kafka.consumer({ groupId: 'robot-odom-group' });

let isKafkaConnected = false;

// ── 3. Connect ─────────────────────────────────────────────────────────────
async function connectKafka() {
  try {
    await Promise.all([
      consumer.connect(),
      odomConsumer.connect(),
    ]);
    isKafkaConnected = true;
    console.log('✅ Kafka Consumers connected');
  } catch (err) {
    isKafkaConnected = false;
    console.error('❌ Kafka connection failed:', err.message);
  }
}

// ── 4. Subscribe ───────────────────────────────────────────────────────────
async function subscribeToTopic(topic) {
  await consumer.subscribe({
    topic,
    fromBeginning: false,
  });
  console.log(`📋 Subscribed to feedback topic "${topic}"`);
}

async function subscribeToOdomTopic(topic) {
  await odomConsumer.subscribe({
    topic,
    fromBeginning: false,
  });
  console.log(`📋 Subscribed to odom topic "${topic}"`);
}

// ── 5. Start Consuming ─────────────────────────────────────────────────────
async function startConsuming() {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const key   = message.key?.toString();
        const value = JSON.parse(message.value.toString());

        console.log(`📥 Received feedback — Topic: ${topic}, ID: ${key}`);
        await processRobotFeedback(key, value);

      } catch (err) {
        console.error('❌ Error processing feedback message:', err.message);
      }
    },
  });
}

async function startOdomConsuming() {
  await odomConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const key   = message.key?.toString();
        const value = JSON.parse(message.value.toString());

        console.log(`📥 Received odom data — Topic: ${topic}, ID: ${key}`);
        await processOdomData(key, value);

      } catch (err) {
        console.error('❌ Error processing odom message:', err.message);
      }
    },
  });
}

// ── 6. Business Logic ──────────────────────────────────────────────────────
async function processRobotFeedback(robotId, feedback) {
  console.log(`⚙️  Processing feedback from robot "${robotId}"`);

  // Route feedback based on type
  switch (feedback.type) {

    case 'STATUS':
      // Robot is sending its current status
      console.log(`🤖 Robot Status — ID: ${robotId}`);
      console.log(`   State    : ${feedback.state}`);       // e.g. "IDLE", "MOVING", "ERROR"
      console.log(`   Battery  : ${feedback.battery}%`);
      console.log(`   Location : x=${feedback.position?.x}, y=${feedback.position?.y}`);
      break;

    case 'TASK_COMPLETE':
      // Robot finished a task
      console.log(`✅ Task Completed — Robot: ${robotId}`);
      console.log(`   Task Name : ${feedback.taskName}`);
      console.log(`   Duration  : ${feedback.duration}s`);
      break;

    case 'TASK_FAILED':
      // Robot failed to complete a task
      console.error(`❌ Task Failed — Robot: ${robotId}`);
      console.error(`   Task Name : ${feedback.taskName}`);
      console.error(`   Reason    : ${feedback.reason}`);
      break;

    case 'PATH_UPDATE':
      // Robot is sending live position updates along a path
      console.log(`📍 Path Update — Robot: ${robotId}`);
      console.log(`   Current Position : x=${feedback.translation?.x}, y=${feedback.translation?.y}`);
      console.log(`   Rotation         : z=${feedback.rotation?.z}, w=${feedback.rotation?.w}`);
      break;

    case 'ERROR':
      // Robot encountered an error
      console.error(`🚨 Robot Error — ID: ${robotId}`);
      console.error(`   Error Code : ${feedback.errorCode}`);
      console.error(`   Message    : ${feedback.message}`);
      break;

    default:
      console.log(`❓ Unknown feedback type "${feedback.type}" from robot "${robotId}"`);
  }
}

// ── 6a. Odom Data Logic ─────────────────────────────────────────────────────
async function processOdomData(robotId, odom) {
  console.log(`🧭 Odom Update — Robot: ${robotId}`);
  
  if (odom.pose && odom.pose.pose) {
    const pos = odom.pose.pose.position;
    const ori = odom.pose.pose.orientation;
    console.log(`   Position    : x=${pos.x.toFixed(3)}, y=${pos.y.toFixed(3)}, z=${pos.z.toFixed(3)}`);
    console.log(`   Orientation : z=${ori.z.toFixed(3)}, w=${ori.w.toFixed(3)}`);
  } else {
    console.log(`   Raw Odom Data:`, odom);
  }
}

// ── 7. Graceful Shutdown ───────────────────────────────────────────────────
async function shutdown() {
  console.log('🔌 Disconnecting consumers...');
  await Promise.all([
    consumer.disconnect(),
    odomConsumer.disconnect(),
  ]);
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ── 8. Bootstrap ───────────────────────────────────────────────────────────
(async () => {
  await connectKafka();

  if (isKafkaConnected) {
    // Start Feedback Consumer
    await subscribeToTopic('robot-feedback');
    startConsuming(); // Run in background

    // Start Odom Consumer
    await subscribeToOdomTopic('ccnt_robot1_odom_with_amcl');
    startOdomConsuming(); // Run in background
  }
})();