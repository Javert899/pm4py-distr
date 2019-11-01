pkill -f launch
git pull
pip3 install -U -r requirements.txt
pip3 install -U pm4pycvxopt
pip3 uninstall -y pm4py
cd ..
cd pm4py-source
git pull
pip3 install -U -r requirements.txt
python3 setup.py install
cd ..
cd pm4py-distr
