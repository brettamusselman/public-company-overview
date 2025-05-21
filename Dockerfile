# Use an official Python runtime as a base image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /src

# Copy requirements.txt from the root of your project into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code into the container (from src/ directory on host to /src inside container)
COPY src/ .

# Expose port (optional; useful for local dev/testing)
EXPOSE 8080

# Command to run the FastAPI app
CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8080"]
