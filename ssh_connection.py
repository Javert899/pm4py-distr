import paramiko
import time

def get_ssh_client(server, username, password):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server, username=username, password=password)
    return ssh

password = input("Insert password: ")
script = input("script ?")

ssh1 = get_ssh_client("137.226.117.71", "berti", password)
ssh2 = get_ssh_client("137.226.117.72", "berti", password)
ssh3 = get_ssh_client("137.226.117.73", "berti", password)
ssh4 = get_ssh_client("137.226.117.74", "berti", password)
ssh5 = get_ssh_client("137.226.117.75", "berti", password)
ssh6 = get_ssh_client("137.226.117.76", "berti", password)

ssh_stdin, ssh_stdout, ssh_stderr = ssh1.exec_command("cd /home/berti/pm4py-distr && echo "+password+" | sudo -S bash "+script)
ssh_stdin, ssh_stdout, ssh_stderr = ssh2.exec_command("cd /home/berti/pm4py-distr && echo "+password+" | sudo -S bash "+script)
ssh_stdin, ssh_stdout, ssh_stderr = ssh3.exec_command("cd /home/berti/pm4py-distr && echo "+password+" | sudo -S bash "+script)
ssh_stdin, ssh_stdout, ssh_stderr = ssh4.exec_command("cd /home/berti/pm4py-distr && echo "+password+" | sudo -S bash "+script)
ssh_stdin, ssh_stdout, ssh_stderr = ssh5.exec_command("cd /home/berti/pm4py-distr && echo "+password+" | sudo -S bash "+script)
ssh_stdin, ssh_stdout, ssh_stderr = ssh6.exec_command("cd /home/berti/pm4py-distr && echo "+password+" | sudo -S bash "+script)

time.sleep(500)
