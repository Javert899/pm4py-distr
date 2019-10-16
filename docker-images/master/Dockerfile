FROM python:3.6

RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get -y install nano vim git python3-pydot python-pydot python-pydot-ng graphviz python3-tk zip unzip curl ftp fail2ban python3-openssl util-linux bsdutils sysstat

RUN pip install --no-cache-dir -U pm4py==1.2.3 requests Flask flask-cors netifaces pm4pycvxopt==0.0.2 psutil

COPY . /

RUN echo "DEFAULT_TYPE = 'master'" >> /pm4pydistr/configuration.py

ENV pm4pydistr--port=5001
#ENV pm4pydistr--master-port=5001
ENV pm4pydistr--base-folders=/master
ENV pm4pydistr--type=master

ENTRYPOINT ["python3", "launch.py"]
