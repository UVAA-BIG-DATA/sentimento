command -v docker > /dev/null 2>&1 || {
  tput setaf 1;
  echo >&2 "Error! require Docker executable to bootstrap this container";
  tput sgr0;
  exit 1;
}

tput setaf 4;
echo "Pulling Zeppelin docker image...";
docker pull xemuliam/zeppelin
docker run -d -p 8000:8888 -p 8443:8445 xemuliam/zeppelin >/dev/null 2>&1;
echo "Running container on port 8080";
tput sgr0;