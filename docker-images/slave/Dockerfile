FROM python:3.8

RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get -y install nano vim git python3-pydot python-pydot python-pydot-ng graphviz python3-tk zip unzip curl ftp fail2ban python3-openssl util-linux bsdutils sysstat

RUN pip install --no-cache-dir -U pm4py==2.0.0 requests Flask flask-cors netifaces psutil pyarrow==1.0.1 pm4pycvxopt==0.0.10

COPY . /

ENV pm4pydistr--port=5002
#ENV pm4pydistr--master-port=5001
ENV pm4pydistr--base-folders=/master
ENV pm4pydistr--type=slave
ENV pm4pydistr--auto-host=1

ENTRYPOINT ["python3", "launch.py"]
