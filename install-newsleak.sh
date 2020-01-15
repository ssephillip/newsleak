#Sets up the folders needed to run the docker-compose setup.
#Prerequisites:
#1. git
#2. docker
#3. docker-compose
git clone https://github.com/ssephillip/newsleak-docker.git
cd newsleak-docker
mkdir -p volumes/ui/conf/dictionaries
mkdir -p volumes/ui/data
mkdir -p volumes/ui/doc2vec-training/result
chmod 777 -R volumes/ui
docker network create hoover_default
docker-compose up -d
docker exec -it -u root newsleak ./newsleak-start.sh
docker-compose stop

echo "--------------------------------------------------"
echo "--------------------------------------------------"
echo "--------------------------------------------------"
echo "Setup finished. You can now start the application."
echo "--------------------------------------------------"
echo "--------------------------------------------------"
echo "--------------------------------------------------"
