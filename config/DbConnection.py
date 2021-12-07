import sys

import mysql.connector

sys.path.append('/home/ravil/Rivigo/PythonScript/Raas-OD-Distance')
import environment


def connect():
    return mysql.connector.connect(
    host=environment.host,
    user=environment.user,
    password=environment.password
    )
