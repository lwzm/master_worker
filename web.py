#!/usr/bin/env python3

import code
import json
import io
import sys
import urllib.parse

import tornado.ioloop
import tornado.options
import tornado.util
import tornado.web


def shell():
    r"""
    >>> sh = shell()
    >>> sh("1 + 1")
    '2\n'
    >>> sh("if True:")
    >>> sh("    1")
    >>> sh("    2")
    >>> sh("")
    '1\n2\n'
    """

    sh = code.InteractiveInterpreter()
    buf = []

    def run(line):
        buf.append(line.rstrip())
        source = "\n".join(buf)
        more = False
        stdout, stderr = sys.stdout, sys.stderr
        output = sys.stdout = sys.stderr = io.StringIO()

        try:
            more = sh.runsource(source)
        finally:
            sys.stdout, sys.stderr = stdout, stderr

        if more:
            return None
        else:
            del buf[:]
            return output.getvalue()

    return run


class BaseHandler(tornado.web.RequestHandler):
    def write_json(self, obj):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.write(json.dumps(obj, default=str, ensure_ascii=False,
                              separators=(",", ":")))

    def write_octet(self, bs, filename):
        self.set_header("Content-Type", "application/octet-stream")
        self.set_header("Content-Disposition",
                        'attachment; filename="{}"'.format(filename))
        self.finish(bs)

    @property
    def query_s(self):
        if not hasattr(self, "_query_s"):
            self._query_s = urllib.parse.unquote(self.request.query)
        return self._query_s

    @property
    def body_s(self):
        if not hasattr(self, "_body_s"):
            self._body_s = self.request.body.decode()
        return self._body_s

    @property
    def kwargs(self):
        if not hasattr(self, "_kwargs"):
            self._kwargs = tornado.util.ObjectDict(
                urllib.parse.parse_qsl((self.request.query)))
        return self._kwargs

    @property
    def json(self):
        if not hasattr(self, "_json"):
            self._json = json.loads(self.request.body.decode())
        return self._json


class MainHandler(BaseHandler):
    def get(self):
        self.render("index.html")

    def post(self):
        self.write_json(eval(self.body_s, None, sys.modules))


class ShellHandler(BaseHandler):
    _sh = shell()
    _sh("import " + ",".join(set(map(
        lambda s: s.split(".")[0], filter(
            lambda s: not s.startswith("_"), sys.modules)))))

    sh = staticmethod(_sh)

    def get(self):
        self.render("shell.html")

    def post(self):
        output = self.sh(self.body_s)
        if output is not None:
            self.write("\n" + output)


class ShowHandler(BaseHandler):
    def get(self):
        s = self.query_s
        if s:
            v = eval(s, None, sys.modules)
            l = dir(v)
        else:
            v = None
            l = sorted(sys.modules)
        self.render("show.html", s=s, v=v, l=l)

    def post(self):
        self.write(repr(eval(self.body_s, None, sys.modules)))


def make_app():
    handlers = [
        (r"/show", ShowHandler),
        (r"/shell", ShellHandler),
        (r"/", MainHandler),
    ]
    return tornado.web.Application(
        handlers,
        static_path="static",
        template_path="templates",
        debug=__debug__,
    )


def run():
    tornado.options.parse_command_line()
    make_app().listen(8000, xheaders=True)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    run()
