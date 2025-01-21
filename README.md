# Bticoin Startup

## Setup
1. Within this directory to add this path as the root run:
   ```bash
   python scripts/setup_env.py
   ```

## Prefect Workflow
This project uses Prefect for workflow orchestration. Here's how to get started:

### Initial Setup
You'll need three terminal windows:

Terminal 1 - Start the Prefect server:
```bash
prefect server start
```

Terminal 2 - Start a worker (required to execute flows):
```bash
prefect worker start -p default-process-pool
```

Terminal 3 - Deploy and run flows:
```bash
# Deploy all flows
prefect deploy

# List available flows
prefect deployment ls

# Run a flow
prefect deployment run [deployment-name]
```

### Development
- Monitor flows in the UI: http://127.0.0.1:4200
- After any code changes, redeploy all flows: `prefect deploy`

### Project Structure
```
stocks/
├── prefect.yaml          # Prefect deployment configurations
├── src/
│   └── flows/           # All Prefect flows
```

### Common Commands
- `prefect server start` - Start the Prefect server
- `prefect deploy` - Deploy/update all flows
- `prefect deployment ls` - List available flows