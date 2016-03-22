#!/usr/bin/env python3

import random
import threading
import time

import master_worker
import web


class T(master_worker.MasterWorker):
    NUM_OF_WORKERS = 32
    def work(self, cmd):
        exec(cmd)


if __name__ == "__main__":
    threading.Thread(target=web.run).start()
    mw = master_worker.instance = T()
    mw.run()
