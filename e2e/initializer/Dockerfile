FROM ubuntu:18.04

ENV ES_ADDRESS=elasticsearch:9200

ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /opt/wait-for-it.sh

COPY etc /etc

RUN \
  chmod +x /opt/wait-for-it.sh && \
  chmod +x /etc/cs-kv-storage/init.sh && \
  apt-get update && \
  apt-get install -y --no-install-recommends curl

VOLUME [ "/etc/cs-kv-storage/configuration" ]

CMD /opt/wait-for-it.sh ${ES_ADDRESS} -s -t 120 -- /etc/cs-kv-storage/init.sh ${ES_ADDRESS}
