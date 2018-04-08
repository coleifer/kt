import atexit
import random
import socket
import subprocess
import sys
import threading
import time

from kt import KyotoTycoon


def run_server(server='ktserver', server_args=None):
    """
    Run ktserver on a random high port and return a client connected to it.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    attempts = 0
    while attempts < 32:
        attempts += 1
        port = random.randint(16000, 32000)
        try:
            sock.bind(('127.0.0.1', port))
            sock.listen(1)
            sock.close()
            time.sleep(0.2)
            break
        except OSError:
            pass

    def _run_server():
        extra_params = server_args or []
        command = [
            server,
            '-le',  # Log errors.
            '-host',
            '127.0.0.1',
            '-port',
            str(port)] + extra_params
        while True:
            p = subprocess.Popen(
                command,
                stderr=sys.__stderr__.fileno(),
                stdout=sys.__stdout__.fileno())
            finished = [False]

            def cleanup():
                finished[0] = True
                p.terminate()
                p.wait()

            atexit.register(cleanup)
            p.wait()
            time.sleep(10)
            if not finished[0]:
                raise Exception('ktserver process died')

    t = threading.Thread(target=_run_server)
    t.daemon = True
    t.start()

    attempts = 0
    while attempts < 20:
        attempts += 1
        try:
            client = KyotoTycoon(host='127.0.0.1', port=port)
            return client
        except:
            time.sleep(0.2)
