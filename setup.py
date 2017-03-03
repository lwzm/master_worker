#!/usr/bin/env python3

import os
from os.path import dirname, abspath, join
import sys
import codecs
from setuptools import setup


HERE = dirname(abspath(__file__))

author = "lwzm"
author_email = "lwzm@qq.com"


def read(*parts):
    with codecs.open(join(HERE, *parts), "rb", "utf-8") as f:
        return f.read()


setup(
    name="master_worker",
    description="master and workers",
    author=author,
    author_email=author_email,
    license="MIT",
    py_modules=["master_worker"],
)
