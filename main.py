import argparse
import math
import struct
import threading
import sys
from socket import *
import json
import os
import time
import pickle
import hashlib

from socket import socket

#Get the parameter of peer IP address from the outer.
hosts = sys.argv[2].split(',')

serverPort = 12002
serverName = hosts
#Global variable for local list of files-to-download.
download_list = list()
block_size = 10240
buffer_size = 10240


#File scanner with infinte loop, detecting file changes. If new,send to connection interface.
def file_scanner():
    save_filedict()
    new_filelist = dict()
    while True:
        time.sleep(1)
        newdict = get_filedict()
#filelist.json is used to save filelist in disk in order to check up updates from different times.
        with open("filelist.json", "r+") as f:
            olddict = json.load(f)
        new = update_filedict(newdict, olddict)
        if new:
            new_filelist.update(new[0])
            save_filedict()
            say_new(new_filelist, serverName[0])
            say_new(new_filelist, serverName[1])

#traverse and read data of files in root share/
def get_filelist(rootdir, filelist):
    if os.path.isfile(rootdir):
        filelist.append(rootdir)

    elif os.path.isdir(rootdir):
        for file in os.listdir(rootdir):
            if '.left' not in file:
                newDir = os.path.join(rootdir, file)
                get_filelist(newDir, filelist)
    return filelist

#make a list of file dictionary for the filelist,with appending file size and last modified time.
def get_filedict():
    filelist = get_filelist('share', [])

    open("filelist.json", "r+")
    filedict = dict()

    for file in filelist:
        current = {file: {"size": os.stat(file).st_size, "mtime": os.stat(file).st_mtime}}
        filedict.update(current)

    return filedict

#if any news or first scanning the dictory, save(update) the filedict as a json on hard disk.
def save_filedict():
    filelist = get_filelist('share', [])

    with open("filelist.json", "w+") as f:
        filedict = dict()

        for file in filelist:
            current = {file: {"size": os.stat(file).st_size, "mtime": os.stat(file).st_mtime}}
            filedict.update(current)
        f.write(json.dumps(filedict))

#compare two filedicts to get the files which A does not have but B have;
# get the same file with different size or last modified time
def update_filedict(newdict, olddict):

    newkey = newdict.keys() - olddict.keys()

    if newkey:
        newfile = [{k: newdict[k]} for k in newkey]

        return newfile




#function that act as a sender of socket.
def send_filelist(addr, filelist):
    filelist_send = pickle.dumps(filelist)
    addr.send(filelist_send)


# -------------------------------TCP LISTENNER--------------------------------------------------------------------------


#TCP Listener with infinite loop. If any connect happens, pass to subconnection.
def TCP_Listen(server_port, filedict):
    server_socket = socket(AF_INET, SOCK_STREAM)
    server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

    server_socket.bind(('', server_port))
    server_socket.listen(20)
    print('The server is ready to receive')
    while True:
        connectionSocket, addr = server_socket.accept()
        print("New connection from", addr)

        handleRequest = threading.Thread(target=subconnection, args=(connectionSocket, addr, filedict))
        handleRequest.daemon = True
        handleRequest.start()

#main part of the server side, which handle different request types
# - hello for inital connection
# - new for new file message
# - get file for file transimission
def subconnection(connectionSocket: socket, addr: tuple, filedict):
    getmessage = connectionSocket.recv(1).decode()

    if getmessage == "0":
        print("Request type: hello")

        filedict_get = pickle.loads(connectionSocket.recv(1024))

        newfile = chekout_newfile(filedict_get, filedict)
        print("new for hello are:")
        if newfile:
            k = None
            for k in newfile:
                new_file = [k, addr[0], 0, 0]
                download_list.append(new_file)

        connectionSocket.send(pickle.dumps(filedict))

    elif getmessage == "1":
        print("Request type: new")

        filedict_get = pickle.loads(connectionSocket.recv(1024))

        newfile = chekout_newfile(filedict_get, filedict)
        if newfile:
            k = None
            for k in newfile:
                new_file = [k, addr[0], 0, 0]
                download_list.append(new_file)


    elif getmessage == "2":
        print("Request type: get file")

        while True:
            msg = connectionSocket.recv(10240)  # Set buffer size as 10kB

            return_msg = msg_parse(msg)
            if return_msg:
                connectionSocket.send(return_msg)


#check out local filedict and remote one, if newfile exists, add to the download list.
def chekout_newfile(filedict_remote, filedict_local):
    if filedict_remote.keys() - filedict_local.keys():
        newfile = filedict_remote.keys() - filedict_local.keys()

        return newfile



def _argparse():
    parser = argparse.ArgumentParser(description="This is description")
    parser.add_argument('--ip', action='store', required=True, dest='ip', help='ip address of peers')
    return parser.parse_args()

#interface of the client side. send entire file list to peers,
def say_hello():
    for ip in serverName:
        clientSocket = socket(AF_INET, SOCK_STREAM)
        clientSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        clientSocket.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        clientSocket.settimeout(30)
        try:
            clientSocket.connect((ip, serverPort))
            clientSocket.settimeout(30)
        except ConnectionRefusedError:
            time.sleep(2)
            say_hello()
        else:
            message = json.dumps(0)
            clientSocket.send(message.encode())
            send_filelist(clientSocket, get_filedict())
            filedict_get = pickle.loads(clientSocket.recv(1024))

            newfile = chekout_newfile(filedict_get, get_filedict())
            print(newfile)
            if newfile:

                    k = None
                    for k in newfile:
                        new_file = [k, ip, 0, 0]
                        download_list.append(new_file)

            time.sleep(2)

#calling from the scanner. send new type request to peers.
def say_new(new_filelist, peer):
    clientSocket = socket(AF_INET, SOCK_STREAM)
    clientSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    clientSocket.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
    clientSocket.settimeout(30)
    try:
        clientSocket.connect((peer, serverPort))
        clientSocket.settimeout(30)
    except ConnectionRefusedError:
        time.sleep(2)
        say_new(new_filelist, peer)
    else:
        message = json.dumps(1)
        clientSocket.send(message.encode())
        send_filelist(clientSocket, new_filelist)
        time.sleep(2)


# --------------------------------DOWNLOADER----------------------------------------------------------------------------

def get_file_md5(filename):
    f = open(filename, 'rb')
    contents = f.read()
    f.close()
    return hashlib.md5(contents).hexdigest()

#infinite loop. check the download list. If any, enter the download module. The downloader records the tickets for each
#file to support resume from the break point.
def downloader():

    while True:

        open("ticket.txt", "a")
        time.sleep(2)
        if download_list:
            if os.path.exists(download_list[0][0]):
                del download_list[0]
                downloader()
            if os.stat("ticket.txt").st_size == 0:
                with open("ticket.txt", "w+") as f:

                    f.write("0")

                    f.close()

            clientSocket = socket(AF_INET, SOCK_STREAM)
            clientSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            clientSocket.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
            clientSocket.settimeout(30)
            try:
                clientSocket.connect((download_list[0][1], serverPort))

            except ConnectionRefusedError:
                time.sleep(2)
                downloader()
            else:
                message = json.dumps(2)
                clientSocket.send(message.encode())
                filename = json.dumps(download_list[0][0])
                filename = eval(filename)
                clientSocket.send(make_get_file_information_header(filename))
                msg = clientSocket.recv(102400)
                file_size, block_size, total_block_number, md5 = parse_file_information(msg)
                download_list[0][3] = total_block_number
                if file_size >= 0:
                    print('Filename:', filename)
                    # Creat a file
                    file = open(filename+".left", 'wb+')

                    # Start to get file blocks
                    with open("ticket.txt", "r") as f:
                        download_list[0][2] = int(f.read())

                        f.close()
                    for block_index in range(download_list[0][2], total_block_number):
                        clientSocket.send(make_get_fil_block_header(filename, block_index))
                        buf = b''
                        if block_index == total_block_number-1:
                            msg = clientSocket.recv(block_size * total_block_number + 100)
                        else:
                            while len(buf) <= block_size :
                                buf += clientSocket.recv(block_size + 100)
                                msg = buf
                        download_list[0][2] = block_index
                        with open("ticket.txt", "w+") as f:
                            f.write(str(download_list[0][2]))

                            f.close()

                        block_index_from_server, block_length, file_block = parse_file_block(msg)
                        file.write(file_block)


                    os.rename(filename+".left", filename)
                    file.close()

                    # Check the MD5
                    md5_download = get_file_md5(filename)
                    if md5_download == md5:
                        print('Downloaded file is completed.')
                        if os.path.exists("ticket.txt"):
                            os.remove("ticket.txt")
                            del download_list[0]
                    else:
                        print('Downloaded file is broken.')
                        if os.path.exists("ticket.txt"):
                            os.remove("ticket.txt")
                            os.remove(filename)
                else:
                    print('No such file.')


def get_file_size(filename):
    return os.path.getsize(filename)


def get_file_block(filename, block_index):

    f = open(filename, 'rb')
    f.seek(block_index * block_size)
    file_block = f.read(block_size)
    f.close()
    return file_block


def make_return_file_information_header(filename):

    if os.path.exists(filename):  # find file and return information
        client_operation_code = 0
        server_operation_code = 0
        file_size = get_file_size(filename)
        total_block_number = math.ceil(file_size / block_size)
        md5 = get_file_md5(filename)
        header = struct.pack('!IIQQQ', client_operation_code, server_operation_code,
                             file_size, block_size, total_block_number)
        header_length = len(header + md5.encode())

        return struct.pack('!I', header_length) + header + md5.encode()

    else:  # no such file
        client_operation_code = 0
        server_operation_code = 1
        header = struct.pack('!II', client_operation_code, server_operation_code)
        header_length = len(header)
        return struct.pack('!I', header_length) + header


def make_file_block(filename, block_index):
    file_size = get_file_size(filename)

    total_block_number = math.ceil(file_size / block_size)

    if os.path.exists(filename) is False:  # Check the file existence
        client_operation_code = 1
        server_operation_code = 1
        header = struct.pack('!II', client_operation_code, server_operation_code)
        header_length = len(header)
        return struct.pack('!I', header_length) + header

    if block_index < total_block_number:
        file_block = get_file_block(filename, block_index)
        client_operation_code = 1
        server_operation_code = 0
        header = struct.pack('!IIQQ', client_operation_code, server_operation_code, block_index, len(file_block))
        header_length = len(header)

        return struct.pack('!I', header_length) + header + file_block
    else:
        client_operation_code = 1
        server_operation_code = 2
        header = struct.pack('!II', client_operation_code, server_operation_code)
        header_length = len(header)
        return struct.pack('!I', header_length) + header


def make_get_file_information_header(filename):
    operation_code = 0
    header = struct.pack('!I', operation_code)
    header_length = len(header + filename.encode())
    return struct.pack('!I', header_length) + header + filename.encode()


def make_get_fil_block_header(filename, block_index):
    operation_code = 1
    header = struct.pack('!IQ', operation_code, block_index)
    header_length = len(header + filename.encode())
    return struct.pack('!I', header_length) + header + filename.encode()


def parse_file_information(msg):
    header_length_b = msg[:4]
    header_length = struct.unpack('!I', header_length_b)[0]
    header_b = msg[4:4 + header_length]
    client_operation_code = struct.unpack('!I', header_b[:4])[0]
    server_operation_code = struct.unpack('!I', header_b[4:8])[0]
    if server_operation_code == 0:  # get right operation code
        file_size, block_size, total_block_number = struct.unpack('!QQQ', header_b[8:32])
        md5 = header_b[32:].decode()
    else:
        file_size, block_size, total_block_number, md5 = -1, -1, -1, ''

    return file_size, block_size, total_block_number, md5


def parse_file_block(msg):
    header_length_b = msg[:4]
    header_length = struct.unpack('!I', header_length_b)[0]
    header_b = msg[4:4 + header_length]
    client_operation_code = struct.unpack('!I', header_b[:4])[0]
    server_operation_code = struct.unpack('!I', header_b[4:8])[0]

    if server_operation_code == 0:  # get right block
        block_index, block_length = struct.unpack('!QQ', header_b[8:24])
        file_block = msg[4 + header_length:]
    elif server_operation_code == 1:
        block_index, block_length, file_block = -1, -1, None
    elif server_operation_code == 2:
        block_index, block_length, file_block = -2, -2, None
    else:
        block_index, block_length, file_block = -3, -3, None

    return block_index, block_length, file_block


def msg_parse(msg):
    header_length_b = msg[:4]
    if len(header_length_b) == 4:
        header_length = struct.unpack('!I', header_length_b)[0]
        header_b = msg[4:4 + header_length]
        client_operation_code = struct.unpack('!I', header_b[:4])[0]

        if client_operation_code == 0:  # get file information
            filename = header_b[4:].decode()
            return make_return_file_information_header(filename)

        if client_operation_code == 1:
            block_index_from_client = struct.unpack('!Q', header_b[4:12])[0]
            filename = header_b[12:].decode()
            return make_file_block(filename, block_index_from_client)

    # Error code
        server_operation_code = 400
        header = struct.pack('!II', client_operation_code, server_operation_code)
        header_length = len(header)
        return struct.pack('!I', header_length) + header

#multiprocessing
if __name__ == '__main__':
    open("filelist.json", "w+")
    t1 = threading.Thread(target=file_scanner)
    t1.start()
    t2 = threading.Thread(target=TCP_Listen, args=(serverPort, get_filedict()))
    t2.start()
    t3 = threading.Thread(target=say_hello)
    t3.start()
    t4 = threading.Thread(target=downloader)
    t4.start()
    while True:
        time.sleep(1)
