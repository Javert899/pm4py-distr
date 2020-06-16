cd /opt/spark/sbin
./start-slave.sh spark://137.226.117.71:7077

if [ "$HOSTNAME" = "padsname1" ]; then
    ./start-master.sh
    sleep 1 && ./start-slave.sh spark://137.226.117.71:7077
else
    sleep 5 && ./start-slave.sh spark://137.226.117.71:7077
fi
