docker run --rm --platform linux/amd64 -it -v .:/build ubuntu:20.04 bash -c '\
  apt-get update &&\
  apt-get install -y build-essential cmake g++ libcurl4-openssl-dev zlib1g-dev &&\
  cd /build/OcapReplaySaver2 &&\
  cmake . &&\
  make &&\
  mkdir -p /build/build/ubuntu_20.04/
  cp OcapReplaySaver2_x64.so /build/build/ubuntu_20.04/'

docker run --rm --platform linux/amd64 -it -v .:/build ubuntu:22.04 bash -c '\
  apt-get update &&\
  apt-get install -y build-essential cmake g++ libcurl4-openssl-dev zlib1g-dev &&\
  cd /build/OcapReplaySaver2 &&\
  cmake . &&\
  make &&\
  mkdir -p /build/build/ubuntu_22.04/
  cp OcapReplaySaver2_x64.so /build/build/ubuntu_22.04/'