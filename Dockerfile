# Use an official Python runtime as a parent image
FROM python:3.6.2
ENV PYTHONUNBUFFERED 1

# Set the working directory to /app
WORKDIR /app

# Install any needed packages specified in requirements.txt
ADD requirements.txt /app/
RUN pip install -r requirements.txt

# Copy the current directory contents into the container at /app
ADD . /app/

EXPOSE 8030 8030

CMD ["python", "listener.py"]
