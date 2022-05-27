FROM ubuntu:latest

LABEL image=dbpro-base-image

RUN apt-get update -qq &&\
    apt-get install -qq -y git gnupg2 wget openjdk-8-jdk scala make g++ make ninja-build

WORKDIR /dbpro

RUN wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
RUN tar -xzf spark-3.2.1-bin-hadoop3.2.tgz 
RUN rm spark-3.2.1-bin-hadoop3.2.tgz
RUN echo "export PATH=$PATH:/dbpro/spark-3.2.1-bin-hadoop3.2/bin" >> ~/.bashrc

RUN 	git clone https://github.com/greenplum-db/gporca.git &&\
	git clone https://github.com/greenplum-db/gp-xerces.git &&\
	git clone https://github.com/apache/calcite.git
RUN 	cd gp-xerces
RUN	mkdir build
RUN	cd build
RUN	../configure --prefix=/usr/local
RUN	make
RUN	make install

RUN 	cd ../../gporca
RUN	cmake -GNinja -H. -Bbuild
RUN	ninja install -C build
