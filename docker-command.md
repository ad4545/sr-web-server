# Restart after code changes
- docker build -t sr-web-server:latest .
- docker service update --image sr-web-server:latest sr-web-server_node-server

# Remove old stack
- docker stack rm sr-web-server
- sleep 5

# Deploy the stack
- docker stack deploy -c docker-compose.yml sr-web-server

# Check status of your stack
- docker stack ps sr-web-server

# Check logs
- docker service logs sr-web-server_node-server

# Scale down to 0 (stops all containers but keeps the stack)
- docker service scale sr-web-server_node-server=0

# Scale back up when ready
- docker service scale sr-web-server_node-server=2

# Remove just the stack
- docker stack rm sr-web-server


