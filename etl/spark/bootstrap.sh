command -v docker > /dev/null 2>&1 || {
  tput setaf 1;
  echo >&2 "Error! require Docker executable to bootstrap this container";
  tput sgr0;
  exit 1;
}

