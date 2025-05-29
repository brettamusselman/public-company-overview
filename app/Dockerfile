FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y gcc g++ build-essential && apt-get clean

# Set working directory
WORKDIR /app

# Copy everything from the current directory
COPY . .

# Upgrade pip and install Python dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Run the app with Gunicorn
CMD gunicorn -b :$PORT app:server

