FROM hseeberger/scala-sbt:8u171_2.12.6_1.1.5

RUN \
  # install docker
  curl -fsSL get.docker.com -o get-docker.sh && \
  sh get-docker.sh && \

  # install docker-compose
  apt-get install python-pip -y && \
  pip install docker-compose
