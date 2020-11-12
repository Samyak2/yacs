import socket
import logging
import sys

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
port = int(sys.argv[1])
w_id = int(sys.argv[2])

logging.info("Starting worker %d on port %d", w_id, port)

master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = "localhost"
master.bind((host, port))
master.listen()

while True:
    conn, addr = master.accept()
    with conn:
        logging.info('Connected by %s', addr)
        data = conn.recv(1024)
        if not data:
            continue
        logging.info("\nPrinting data: %s \n", data.decode('utf-8'))
