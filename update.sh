pkill -f custom_launch
git clean -x -f
git reset --hard HEAD
rm -rRf slave*
rm -f nohup.out
git pull
pip3 uninstall ortools
pip3 install -U pm4pydistr
pip3 install -U -r requirements.txt
pip3 uninstall pm4py
pip3 install -U pm4pyexperimental
nohup python3 select_worker.py &
pip3 install -U cvxopt
pip3 install --no-deps pm4pycvxopt==0.0.9
