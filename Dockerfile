FROM python:3.11-slim

# Install Java as Pyspark requires Java to run 
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

 # Setting the working directory to /app
WORKDIR /app   

# Installing Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copying the project files into the container 
COPY . .

# Expose JupyterLab port
EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]
# Basically when the container starts, JuputerLab will be automatically launched and accesible on port 8888 without requiring a token or password. 
