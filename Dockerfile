# Step 1: Use a base Python image (python:3.9-slim for a lightweight image)
FROM python:3.10-slim

# Step 2: Set the working directory inside the container
WORKDIR /BELmc

# Step 3: Copy all the project files into the container's working directory
COPY . /BELmc

# Step 4: Install the dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Step 5: Set the entry point to your main script
CMD ["python", "collector_ems_temp.py"]
