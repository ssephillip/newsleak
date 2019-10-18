#Sets up the folders needed to run the docker-compose setup.
#Prerequisites:
#1. git
#2. docker
#3. docker-compose
git clone https://github.com/ssephillip/newsleak-docker.git
cd newsleak-docker
docker network create hoover_default
docker-compose up -d
docker exec -it -u root newsleak ./newsleak-start.sh
docker-compose stop
echo "Setup finished. You can now start the application."
