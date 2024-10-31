FROM adoptopenjdk:11.0.8_10-jre-hotspot-bionic@sha256:24864d2d79437f775c70fd368c0272a1579a45a81c965e5fdcf0de699c15a054

RUN set -ex; \
  export DEBIAN_FRONTEND=noninteractive; \
  apt-get update; \
  mkdir -p /opt/eventsim

WORKDIR /opt/eventsim

# Copy necessary files
COPY eventsim.sh /opt/eventsim/eventsim.sh
COPY examples /opt/eventsim/examples
COPY data /opt/eventsim/data
COPY eventsim-assembly-2.0.jar /opt/eventsim/eventsim-assembly-2.0.jar

# Set executable permissions for the script
RUN chmod +x /opt/eventsim/eventsim.sh

# Set the entry point
ENTRYPOINT ["/opt/eventsim/eventsim.sh"]
