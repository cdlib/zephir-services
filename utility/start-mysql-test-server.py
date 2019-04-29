import os
import subprocess

try:
    # create a tmp directory for all of the mysql data
    mktemp_output = subprocess.check_output(
        "MYSQL_DATA=$(mktemp -d /tmp/MYSQL-XXXXX); echo $MYSQL_DATA", shell=True
    )
    MYSQL_DATA = mktemp_output.decode("utf-8").strip()
    # create a tmp log file
    subprocess.check_output("touch {0}/out.log".format(MYSQL_DATA), shell=True)
    # start the mysql server
    mysqld_output = subprocess.check_output(
        "mysqld --datadir={0} --pid-file={0}/mysql.pid --socket={0}/mysql.socket --skip-networking --skip-grant-tables &> {0}/out.log &".format(
            MYSQL_DATA
        ),
        shell=True,
    )

    # export the socket (will be used by mysql to connect) and pid file (will be used for shutdown)
    print(
        "export MYSQL_UNIX_PORT={0}/mysql.socket; export MYSQL_PID_FILE={0}/mysql.pid;\n".format(
            MYSQL_DATA
        )
    )
    os.environ["MYSQL_UNIX_PORT"] = "{0}/mysql.socket".format(
        MYSQL_DATA
    )

    print("echo 'Mysql test server started'")
except subprocess.CalledProcessError as grepexc:
    print("echo '", grepexc.returncode, grepexc.output, "'")
