pkill -f launch
git clean -x -f
git reset --hard HEAD
rm -rRf slave*
rm -f nohup.out
git pull
pip3 install -U -r requirements.txt
pip3 install -U pm4pycvxopt
pip3 install --no-deps pm4pyigraph
pip3 uninstall -y pm4py
cd ..
cd pm4py-source
git clean -x -f
git reset --hard HEAD
rm -rRf build dist pm4py.egg-info
git pull
pip3 install -U -r requirements.txt
python3 setup.py install
rm -rRf build dist pm4py.egg-info
cd ..
cd pm4py-distr
nohup python3 select_worker.py &
