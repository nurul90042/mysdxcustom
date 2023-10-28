#!/usr/bin/env python3
# Author:
# Muhammad Shahbaz (muhammad.shahbaz@gatech.edu)

from threading import Thread
from multiprocessing import Queue
from multiprocessing.connection import Listener

''' bgp server '''
class Server():

    def __init__(self):
        self.listener = Listener(('localhost', 6000), authkey=b'xrs')

        self.sender_queue = Queue()
        self.receiver_queue = Queue()

    def start(self):
        self.conn = self.listener.accept()
        print('Connection accepted from', self.listener.last_accepted)

        self.sender = Thread(target=_sender, args=(self.conn, self.sender_queue))
        self.sender.start()

        self.receiver = Thread(target=_receiver, args=(self.conn, self.receiver_queue))
        self.receiver.start()

''' sender '''
def _sender(conn, queue):
    while True:
        try:
            line = queue.get()
            conn.send(line)
        except Exception as e:
            print(e)
            pass

''' receiver '''
def _receiver(conn, queue):
    while True:
        try:
            line = conn.recv()
            queue.put(line)
        except Exception as e:
            print(e)
            pass

''' main ''' 
if __name__ == '__main__':
    while True:
        server_instance = Server()

        while True:
            try:
                print(server_instance.receiver_queue.get())
                server_instance.sender_queue.put('announce route %s next-hop %s as-path [ %s ]' % ('200.0.0.0/16', '172.0.0.1', '100'))
            except Exception as e:
                print(e)
                print('thread ended')
                break
