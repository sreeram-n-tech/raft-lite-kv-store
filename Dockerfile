FROM python:3.14-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir grpcio requests

# Copy source code
COPY . .

# Run the node
ENTRYPOINT ["python", "-m", "cmd.kvnode.main"]
