#!/usr/bin/env python3

import random
import threading
import time

import master_worker


class T(master_worker.MasterWorker):
    NUM_OF_WORKERS = 32


if __name__ == "__main__":
    import tornadospy
    tornadospy.run_in_thread()
    T.instance().run()
