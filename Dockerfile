FROM ubuntu

RUN  apt-get update &&  apt-get install -y python3 && \
     apt-get install -y pip && \
     pip install kafka-python==2.0.1 emoji tweepy==3.10.0 pyspark==2.4.5

COPY ./code /home/code/