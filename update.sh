pkill -f launch
git clean -x -f
git reset --hard HEAD
rm -rRf slave*
git pull
pip3 install -U -r requirements.txt
pip3 install -U pm4pycvxopt
pip3 uninstall -y pm4py
cd ..
cd pm4py-source
git clean -x -f
git reset --hard HEAD
rm -rRf build dist
git pull
pip3 install -U -r requirements.txt
python3 setup.py install
rm -rRf build dist
cd ..
cd pm4py-distr
nohup python3 select_worker.py &
