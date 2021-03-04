pkill -f custom_launch
git clean -x -f
git reset --hard HEAD
rm -rRf slave*
rm -f nohup.out
git pull
pip3 uninstall ortools
pip3 install -U pm4pydistr
pip3 install -U -r requirements.txt
pip3 install -U cvxopt
pip3 install -U pm4pycvxopt==0.0.11
