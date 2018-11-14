command -v docker > /dev/null 2>&1 || {
  tput setaf 1;
  echo >&2 "Error! require Docker executable to bootstrap this container";
  tput sgr0;
  exit 1;
}

tput setaf 4;
echo "Pulling Cassandra docker image...";
docker pull cassandra
docker run --name local-cassandra -v /tmp/cassandra:/var/lib/cassandra -d cassandra:tag
 >/dev/null 2>&1;
echo "Running container on port ";
tput sgr0;