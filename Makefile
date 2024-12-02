DOCKER_NETWORK = hadoop-network
ENV_FILE = hadoop.env
VERSION_REPOSITORY_DOCKER = 0.0.3

current_branch := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "default-branch")

# Realiza criacao da REDE DOCKER_NETWORK
create-network:
	docker network create hadoop_network

  # ################## 1.3 #####################
  # APP { SILVER GOOGLE PLAY}
  # ############################################

build-app-silver-reviews-google-play:
	docker build -t iamgacarvalho/dmc-app-silver-reviews-google-play:$(VERSION_REPOSITORY_DOCKER)  ./application/google-play-processing-historical
	docker push iamgacarvalho/dmc-app-silver-reviews-google-play:$(VERSION_REPOSITORY_DOCKER) 

restart-docker:
	sudo systemctl restart docker

down-services:
	docker rm -f $(docker ps -a -q)

