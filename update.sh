pkill -f launch
git clean -x -f
git reset --hard HEAD
rm -rRf slave*
rm -f nohup.out
git pull
pip3 install -U -r requirements.txt
nohup python3 select_worker.py &
