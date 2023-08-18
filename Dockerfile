FROM apache/airflow:2.6.3 
ADD requirements.txt . 
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt 
ADD google-private-key.json .


# Installing the GCP CLI in the container
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER 0
RUN chmod 774 google-private-key.json
ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk
ENV GOOGLE_APPLICATION_CREDENTIALS=google-private-key.json
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

ENV AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com 
ENV AIRFLOW__SMTP__SMTP_STARTTLS=True
ENV AIRFLOW__SMTP__SMTP_SSL=False
ENV AIRFLOW__SMTP__SMTP_USER=priyegeorge1st@gmail.com
ENV AIRFLOW__SMTP__SMTP_PASSWORD=hdwtsgoshdcghytu
ENV AIRFLOW__SMTP__SMTP_PORT=25
ENV AIRFLOW__SMTP__SMTP_MAIL_FROM=airflow-task@test.com

# RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
#     && TMP_DIR="$(mktemp -d)" \
#     && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
#     && mkdir -p "${GCLOUD_HOME}" \
#     && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
#     && "${GCLOUD_HOME}/install.sh" \
#     --bash-completion=false \
#     --path-update=false \
#     --usage-reporting=false \
#     --quiet \
#     && rm -rf "${TMP_DIR}" \
#     && gcloud --version


# 1 Make directory in root named .google
# 2 move our certificate to that newly created directory and rename them
# RUN cd ~ && mkdir -p ~/.google/credentials/
# COPY ./google-private-key.json ~/.google/credentials/google-private-key.json
# RUN ls  ~/.google/credentials/


