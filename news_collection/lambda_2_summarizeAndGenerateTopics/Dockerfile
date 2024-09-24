# Use the official AWS Lambda Python 3.12 base image
FROM public.ecr.aws/lambda/python:3.12

# Set the working directory inside the container
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy only the requirements.txt file to the container first
COPY requirements.txt .

# Install dependencies. This step will be cached by Docker as long as requirements.txt does not change.
RUN pip install -r requirements.txt --no-cache-dir

# Copy the rest of the application code to the working directory
COPY . .

# Ensure the correct permissions on the copied files
RUN chmod -R 755 ${LAMBDA_TASK_ROOT}

# Set the command to your function handler (app.lambda_handler)
CMD ["lambda_function.lambda_handler"]
