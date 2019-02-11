import os
import subprocess

try:
    mktemp_output = subprocess.check_output("MYSQL_DATA=$(mktemp -d /tmp/MYSQL-XXXXX); echo $MYSQL_DATA", shell=True)
    MYSQL_DATA = mktemp_output.decode("utf-8").strip()
    subprocess.check_output("touch {0}/out.log".format(MYSQL_DATA), shell=True);
    mysqld_output = subprocess.check_output("mysqld --datadir={0} --pid-file={0}/mysql.pid --socket={0}/mysql.socket --skip-networking --skip-grant-tables &> {0}/out.log &".format(MYSQL_DATA), shell=True, env=dict(os.environ))
    print("export MYSQL_UNIX_PORT={0}/mysql.socket; export MYSQL_PID_FILE={0}/mysql.pid;\n".format(MYSQL_DATA))
    print("echo 'Mysql test server started'")
except subprocess.CalledProcessError as grepexc:
    print("echo '", grepexc.returncode, grepexc.output,"'")
