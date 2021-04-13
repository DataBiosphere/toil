#!/usr/bin/env python3
import argparse
import sys
import subprocess
from typing import List


def make_parser():
    p = argparse.ArgumentParser()
    p.add_argument("progargs", nargs=argparse.REMAINDER, help="The program and its arguments")
    p.add_argument("--num", type=int, help="number of times to run the application")
    p.add_argument("--no-fail", help="add this flag to actually work", action="store_true")
    return p


class Runner:
    def __init__(self):
        if sys.stdin.isatty():
            self.indata = None
        else:
            self.indata = sys.stdin.read().encode(sys.stdin.encoding)

    def run_once(self, args: List[str]):
        subprocess.run(args, input=self.indata, stdout=sys.stdout, stderr=sys.stderr).check_returncode()

    def run_many(self, n: int, args: List[str]):
        for i in range(n):
            self.run_once(args)


if __name__ == "__main__":
    args = make_parser().parse_args()
    assert args.no_fail is True, "Didn't set the --no-fail flag"
    r = Runner()
    r.run_many(args.num, args.progargs)
