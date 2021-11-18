# Install base Python image
FROM python:3.8-slim-buster

# Copy files to the container
COPY *.py /app/
COPY requirements.txt /app/

# Set working directory to previously added app directory
WORKDIR /app/

# Install dependencies
RUN pip install -r requirements.txt


# Get pretrined model and download it to work dir
RUN python model_getter.py

# Expose the port uvicorn is running on
EXPOSE 80

# Run uvicorn server
CMD ["uvicorn", "server:app", "--reload", "--host", "0.0.0.0", "--port", "80"]