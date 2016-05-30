#!/usr/bin/env python3

import collections
import datetime
import os
import pickle
import multiprocessing
import resource
import signal
import struct
import sys
import time


class MasterWorker(object):
    """Multiprocessing based master-worker model. Singleton(lock todo)
    """

    NUM_OF_WORKERS = 4
    RLIMIT_CPU = 60

    def __init__(self):
        self.children = set()
        self.history = collections.deque(maxlen=500)
        self.returns = collections.deque(maxlen=1000)
        self._struct_msg_header = struct.Struct("!HH")
        self._lock_w = multiprocessing.Lock()
        self._pipe_r, self._pipe_w = os.pipe()
        os.set_blocking(self._pipe_r, False)  # set_blocking to _pipe_w is unnecessary
        signal.signal(signal.SIGCHLD, self._sig_chld)
        signal.signal(signal.SIGTERM, self._sig_term)
        self.init()

    def _sig_chld(self, signum, frame):
        while True:
            try:
                pid, status = os.waitpid(0, os.WNOHANG)
                if pid == 0:
                    break
                exit_status, signal_number = status.to_bytes(2, "big")
                self.children.discard(pid)
                self.history.append((
                    datetime.datetime.now(), "end", pid,
                    exit_status, signal_number,
                ))

                pid_, msg_size = self._struct_msg_header.unpack(
                    os.read(self._pipe_r, self._struct_msg_header.size))
                msg = pickle.loads(os.read(self._pipe_r, msg_size))
                self.returns.append(msg)
            except ChildProcessError:
                break

    def _sig_term(self, signum, frame):
        while self.children:
            pid, status = os.wait()
            self.children.discard(pid)
        sys.exit()

    def run(self):
        while True:
            #print(self.returns)
            while True:
                if len(self.children) < self.NUM_OF_WORKERS:
                    break
                time.sleep(0.1)

            cmd = self.get_command()
            pid = os.fork()
            if pid == 0:  # child
                signal.signal(signal.SIGCHLD, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                resource.setrlimit(resource.RLIMIT_CPU, (self.RLIMIT_CPU, -1))
                try:
                    result = pickle.dumps(self.work(cmd))
                    if len(result) > 16 * 1024:
                        raise ValueError("pickled too long", len(result), type(result))
                except Exception as e:
                    result = pickle.dumps(e)
                with self._lock_w:
                    os.write(self._pipe_w, self._struct_msg_header.pack(os.getpid(), len(result)))
                    os.write(self._pipe_w, result)
                sys.exit()
            else:
                self.children.add(pid)
                self.history.append((
                    datetime.datetime.now(), "begin", pid,
                ))

    def init(self):
        pass

    def get_command(self) -> object:
        """Just an example

        subclass should override this
        """

        return input("> ")

    def work(self, cmd) -> object:
        """Just an example

        subclass should override this
        """

        return eval(cmd)


def log(*args):
    print(datetime.datetime.now(), *args, file=sys.stderr)


def main():
    import random
    import threading

    def _term_self(signum, frame):
        print("SIGTERM")
        os.kill(os.getpid(), signal.SIGTERM)

    class T(MasterWorker):
        def work(self, cmd):
            random.seed()
            delay = random.random()
            time.sleep(delay)
            exec(cmd)
            return delay


    signal.signal(signal.SIGINT, _term_self)
    MasterWorker().run()


if __name__ == "__main__":
    main()
