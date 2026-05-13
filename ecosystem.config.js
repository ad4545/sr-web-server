module.exports = {
  apps: [
    {
      name: "sr-api-core",
      script: "npm",
      args: "run start:api-core",
      cwd: __dirname,
    },
    {
      name: "sr-realtime-core",
      script: "npm",
      args: "run start:realtime-core",
      cwd: __dirname,
    },
    {
      name: "sr-nginx",
      script: "bash",
      args: "scripts/start-nginx.sh",
      cwd: __dirname,
    },
  ],
};
