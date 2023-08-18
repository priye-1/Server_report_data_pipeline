#Update the installed packages and package cache on your instance
sudo yum update -y

#Install docker 
sudo yum install docker

# install docker compose
sudo apt-get install docker-compose-plugin

# start docker service
sudo service docker start

# Add the ec2-user to the docker group so you can execute Docker commands without using sudo
sudo usermod -a -G docker ec2-user

# confirm docker is running
sudo service docker status

#Verify that the ec2-user can run Docker commands without sudo
docker info

# get airflow docker compose file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml'

# initialize database
docker compose up airflow-init

# start all docker services
docker compose up


################################################################### Setting up bigquery ##################################################################################
# install gcloud cli - https://cloud.google.com/sdk/docs/install and then initialize - https://cloud.google.com/sdk/docs/initializingg


# create service account
gcloud iam service-accounts create SA_NAME \
    --description="DESCRIPTION" \
    --display-name="DISPLAY_NAME"

# grant your service account an IAM role on your projec
gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="serviceAccount:SA_NAME@PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/ROLE_NAME"

# create service account keys
gcloud iam service-accounts keys create google-private-key.json \
    --iam-account=SA_NAME@PROJECT_ID.iam.gserviceaccount.com