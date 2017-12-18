import sys
from omero.cli import BaseControl, CLI, ExceptionHandler

import h5py
import numpy as np

HELP = """Plugin for importing IDR ROIs"""

class IDROIControl(BaseControl):
    """
    Some documentation
    """

    def _configure(self, parser):
        # Add an exception handler
        self.exc = ExceptionHandler()

        # Add default login arguments, prompting the user for
        # server login credentials
        parser.add_login_arguments()

        # Add some 'commands', i.e. operations the plugin can perform
        parser.add_argument(
            "command", nargs="?",
            choices=("import", "remove", "parse"),
            help="The operation to be performed")

        parser.add_argument(
            "file",
            help="The HDF5 file")

        # Add an additional argument
        parser.add_argument(
            "--some_argument", help="An additional argument")

        parser.set_defaults(func=self.process)

    def process(self, args):
        # Check for necessary arguments
        if not args.command:
            # Exit with code 100
            self.ctx.die(100, "No command provided")

        if args.command == "import":
            self.importFile(args)

        if args.command == "remove":
            self.remove(args)

        if args.command == "parse":
            self.parse(args, updateService=None)

    def parse(self, args, updateService):
        print("Parse file %s" % args.file)
        h5f = h5py.File(args.file, "r", libver="latest")
        try:
            imgs = h5f['Images']
            objs = h5f['Objects']
            print("imgs")
            print(imgs.items())
            print(imgs.keys())
            print(imgs.values())
            print("objs")
            print(objs.items())
            print(objs.keys())
            print(objs.values())
        finally:
            h5f.close()

    def importFile(self, args):
        print("Import from file %s" % args.file)
        conn = self.ctx.conn(args)
        updateService = conn.sf.getUpdateService()
        self.parse(args, updateService)

    def remove(self, args):
        print("Remove")

try:
    register("idroi", IDROIControl, HELP)
except NameError:
    if __name__ == "__main__":
        cli = CLI()
        cli.register("idroi", IDROIControl, HELP)
        cli.invoke(sys.argv[1:])