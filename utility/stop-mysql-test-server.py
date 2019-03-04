import os
import subprocess

try:
    MYSQL_PID_FILE = os.environ['MYSQL_PID_FILE']
    mktemp_output = subprocess.check_output("kill -9 `cat {}`".format(MYSQL_PID_FILE), shell=True)
    print("unset MYSQL_UNIX_PORT; unset MYSQL_PID_FILE;\n")
    print("echo 'Mysql test server stopped'")
except subprocess.CalledProcessError as grepexc:
    print("echo '", grepexc.returncode, grepexc.output,"'")
