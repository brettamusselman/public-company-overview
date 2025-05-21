FROM python:3.12.6-slim

# Set the working directory to /app
WORKDIR /app

# Copy only the requirements first to cache dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY src/ ./src/

# Set working dir to src so main.py can be run relative to it
WORKDIR /app/src

# Run the API server
CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8080"]
