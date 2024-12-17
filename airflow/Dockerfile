# Gunakan image resmi Apache Airflow sebagai basis
FROM apache/airflow:2.6.2-python3.9

# Set environment variable untuk mengurangi interaksi selama proses build
ENV AIRFLOW_HOME=/opt/airflow

# Copy file konfigurasi atau kebutuhan ke dalam container
COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt

# Install library tambahan yang dibutuhkan
RUN pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements.txt

# Ubah user ke airflow
USER airflow

# Jalankan entrypoint default Airflow
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["bash", "-c", "airflow db init && airflow webserver"]
