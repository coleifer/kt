import atexit
import logging
import random
import socket
import subprocess
import sys
import threading
import time

from .client import KyotoTycoon
from .exceptions import KyotoTycoonError


logger = logging.getLogger(__name__)


class EmbeddedServer(object):
    def __init__(self, server='ktserver', host='127.0.0.1', port=None,
                 database='*', server_args=None):
        self._server = server
        self._host = host
        self._port = port
        self._database = database
        self._server_args = server_args or []

        # Signals for server startup and shutdown.
        self._server_started = threading.Event()
        self._server_terminated = threading.Event()
        self._server_terminated.set()  # Start off in terminated state.

        # Placeholders for server process and client.
        self._server_p = None
        self._client = None

    @property
    def client(self):
        if self._server_terminated.is_set():
            raise KyotoTycoonError('server not running')
        elif self._client is None:
            self._client = KyotoTycoon(self._host, self._port)
        return self._client

    @property
    def pid(self):
        if not self._server_terminated.is_set():
            return self._server_p.pid

    def _run_server(self, port):
        command = [
            self._server,
            '-le',  # Log errors.
            '-host',
            self._host,
            '-port',
            str(port)] + self._server_args + [self._database]

        while not self._server_terminated.is_set():
            self._server_p = subprocess.Popen(
                command,
                stderr=sys.__stderr__.fileno(),
                stdout=sys.__stdout__.fileno())

            self._server_started.set()
            self._server_p.wait()
            self._client = None

            time.sleep(0.1)
            if not self._server_terminated.is_set():
                logger.error('server process died, restarting...')

        logger.info('server shutdown')

    def _stop_server(self):
        self._server_terminated.set()
        self._server_p.terminate()
        self._server_p.wait()
        self._server_p = self._client = None

    def run(self):
        """
        Run ktserver on a random high port and return a client connected to it.
        """
        if not self._server_terminated.is_set():
            logger.warning('server already running')
            return False

        if self._port is None:
            self._port = self._find_open_port()

        self._server_started.clear()
        self._server_terminated.clear()

        t = threading.Thread(target=self._run_server, args=(self._port,))
        t.daemon = True
        t.start()

        self._server_started.wait()  # Wait for server to start up.
        atexit.register(self._stop_server)

        attempts = 0
        while attempts < 20:
            attempts += 1
            try:
                self._client = KyotoTycoon(host=self._host, port=self._port)
                return True
            except socket.error:
                time.sleep(0.1)
            except OSError:
                time.sleep(0.1)

        self._stop_server()
        raise KyotoTycoonError('Unable to connect to server on %s:%s' %
                               (self._host, self._port))

    def stop(self):
        if self._server_terminated.is_set():
            logger.warning('server already stopped')
            return False

        if hasattr(atexit, 'unregister'):
            atexit.unregister(self._stop_server)
        else:
            funcs = []
            for fn, arg, kw in atexit._exithandlers:
                if fn != self._stop_server:
                    funcs.append((fn, arg, kw))
            atexit._exithandlers = funcs
        self._stop_server()

    def _find_open_port(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        attempts = 0
        while attempts < 32:
            attempts += 1
            port = random.randint(16000, 32000)
            try:
                sock.bind(('127.0.0.1', port))
                sock.listen(1)
                sock.close()
                time.sleep(0.1)
                return port
            except OSError:
                pass

        raise KyotoTycoonError('Could not find open port')
