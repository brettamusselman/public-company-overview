# Use an official Python runtime as a base image
FROM python:3.12-slim

# Set working directory inside container
WORKDIR /src

# Copy requirements from root of project into container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code from host's src/ to container's /src
COPY . .

# Expose port
EXPOSE 8080

# Run the FastAPI app
CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8080"]
