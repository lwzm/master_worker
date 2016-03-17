#!/usr/bin/env python3

import collections
import datetime
import multiprocessing
import os
import resource
import signal
import sys
import time


class MasterWorker(object):
    """Multiprocessing based master-worker model
    """

    NUM_OF_WORKER = 4
    RLIMIT_CPU = 60

    def __init__(self):
        self.children = set()
        self.history = collections.deque(maxlen=500)
        self.lock = multiprocessing.Lock()
        signal.signal(signal.SIGCHLD, self._sig_chld)

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
            except ChildProcessError:
                break

    def run(self):
        while True:
            while True:
                if len(self.children) < self.NUM_OF_WORKER:
                    break
                time.sleep(0.1)

            cmd = self.get_command()
            pid = os.fork()
            if pid == 0:  # child
                resource.setrlimit(resource.RLIMIT_CPU, (self.RLIMIT_CPU, -1))
                self.work(cmd)
                sys.exit()
            else:
                self.children.add(pid)
                self.history.append((
                    datetime.datetime.now(), "begin", pid,
                ))

    def get_command(self) -> object:
        """Just an example

        subclass should override this
        """

        return input("> ")

    def work(self, cmd) -> None:
        """Just an example

        subclass should override this
        """

        repr(cmd)


def log(*args):
    print(datetime.datetime.now(), *args, file=sys.stderr)


def main():
    import random
    import threading

    class T(MasterWorker):
        def get_command(self):
            print(self.children)
            return super().get_command()

        def work(self, cmd):
            time.sleep(random.random())
            log(cmd)

    master_worker = T()
    # master_worker.run()
    thd = threading.Thread(target=master_worker.run)
    thd.start()

    def _term_self(signum, frame):
        os.kill(os.getpid(), signal.SIGTERM)

    signal.signal(signal.SIGINT, _term_self)

    while True:
        time.sleep(0.01)


if __name__ == "__main__":
    main()
