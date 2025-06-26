# Use the official Python image as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set environment variables (optional)
# ENV PYTHONUNBUFFERED=1

# Default command (can be changed as needed)
CMD ["python3", "utilities/load_historical_options_data.py"]
