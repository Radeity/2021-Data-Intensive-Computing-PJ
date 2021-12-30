# uncompyle6 version 3.7.4
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.6.9 (default, Oct  8 2020, 12:12:24) 
# [GCC 8.4.0]
# Embedded file name: stream.py
# Compiled at: 2020-10-26 18:12:17
# Size of source mod 2**32: 1125 bytes
import socket, sys, requests, json, time, csv, cryptography
from cryptography.fernet import Fernet
key = b'TXG4f9eYOgV2F_Wkt4-AAg1xgmXafvVnCF9XNTCvKbg='
fer = Fernet(key)

def send_data_to_spark(tcp_connection, eve, mat):
    m = eve[0]['matchId']
    fl = 0
    msg = ''
    for i in eve:
        if i['matchId'] == m:
            msg = json.dumps(i)
        else:
            fl = 0
            time.sleep(5)
            m = i['matchId']
            msg = json.dumps(i)
        if fl == 0:
            fl = 1
            for j in mat:
                if j['wyId'] == m:
                    tcp_connection.send((json.dumps(j)+"\n").encode())
                    break
        # print(msg)
        tcp_connection.send((msg+"\n").encode())
        # time.sleep(2)
    '''
    msg="I am sreyabsaabdjasbfakjfa"
    '''

TCP_IP = 'localhost'
TCP_PORT = 6100
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

with open('eve.txt', 'rb') as (f):
    t = f.read()
    da = fer.decrypt(t)
    js1 = json.loads(da)
    # with open('decode_eve.txt', 'w') as (f1):
    #     json.dump(js1, f1, indent=4)


with open('mat.txt', 'rb') as (f):
    t = f.read()
    da = fer.decrypt(t)
    js2 = json.loads(da)
    # with open('decode_mat.txt', 'w') as (f2):
    #     json.dump(js2, f2, indent=4)

time.sleep(2)
print('Waiting for connection...')
conn, addr = s.accept()
print('Connected... Starting to push EPL data')
send_data_to_spark(conn, js1, js2)
# okay decompiling /home/sreyans/Desktop/SEM5/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/stream/stream.pyc
