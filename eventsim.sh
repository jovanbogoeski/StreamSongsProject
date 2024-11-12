#! /bin/bash

# Run the Java application in a loop
while true; do
  java -XX:+UseG1GC -XX:+UseStringDeduplication -Xmx8G -jar /opt/eventsim/eventsim-assembly-2.0.jar "$@"
  sleep 5
done

# Keep the container running with tail command
tail -f /dev/null