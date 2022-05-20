#!/usr/bin/env python3
# coding:utf-8

# default libraries
import sys

if sys.version_info < (3, 6):  # minimum is python3.6
    raise AssertionError("Minimum python version to run HDF5_2_JSON is 3.6. Your current python version is " +
                         ".".join(map(str, sys.version_info)))
import argparse
import logging
import pkg_resources
import tempfile
import os

# local modules
import hdf5_2_json.formats

def checkTsvSanity(tsv):
    f = open(tsv, "r")
    nameSet = set()
    duplicatedNames = set()
    nonExistingFiles = set()
    for line in f:
        elements = [el.strip() for el in line.split("\t")]
        if len(elements) <= 1:
            raise Exception(f"No tabulation separator found in given file: {tsv}")
        if " " in elements[0]:
            raise Exception(f"Your genome names contain spaces (The first encountered genome name that had this string:"
                            f" '{elements[0]}'). To ensure compatibility with all of the dependencies of PPanGGOLiN "
                            f"this is not allowed. Please remove spaces from your genome names.")
        oldLen = len(nameSet)
        nameSet.add(elements[0])
        if len(nameSet) == oldLen:
            duplicatedNames.add(elements[0])
        if not os.path.exists(elements[1]):
            nonExistingFiles.add(elements[1])
    if len(nonExistingFiles) != 0:
        raise Exception(f"Some of the given files do not exist. The non-existing files are the following : "
                        f"'{' '.join(nonExistingFiles)}'")
    if len(duplicatedNames) != 0:
        raise Exception(
            f"Some of your genomes have identical names. The duplicated names are the following : "
            f"'{' '.join(duplicatedNames)}'")


def checkInputFiles(anno=None, pangenome=None, fasta=None):
    """
        Checks if the provided input files exist and are of the proper format
    """
    if pangenome is not None and not os.path.exists(pangenome):
        raise FileNotFoundError(f"No such file or directory: '{pangenome}'")

    if anno is not None:
        if not os.path.exists(anno):
            raise FileNotFoundError(f"No such file or directory: '{anno}'")
        checkTsvSanity(anno)

    if fasta is not None:
        if not os.path.exists(fasta):
            raise FileNotFoundError(f"No such file or directory: '{fasta}'")
        checkTsvSanity(fasta)


def checkLog(name):
    if name == "stdout":
        return sys.stdout
    elif name == "stderr":
        return sys.stderr
    else:
        return open(name, "w")


def cmdLine():
    # need to manually write the description so that it's displayed into groups of subcommands ....
    desc = "\n"
    desc += "HDF5 2 JSON\n"
    desc += "    write         Writes 'flat' files representing the pangenome that can be used with other software\n"
    desc += "  \n"

    parser = argparse.ArgumentParser(
        description="HDF5_2_JSON",
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s ' + pkg_resources.get_distribution("hdf5_2_json").version)
    subparsers = parser.add_subparsers(metavar="", dest="subcommand", title="subcommands", description=desc)
    subparsers.required = True  # because python3 sent subcommands to hell apparently

    subs = [hdf5_2_json.formats.writeFlat.writeFlatSubparser(subparsers)]  # subparsers

    for sub in subs:  # add options common to all subcommands
        common = sub._action_groups.pop(1)  # get the 'optional arguments' action group.
        common.title = "Common arguments"
        common.add_argument("--tmpdir", required=False, type=str, default=tempfile.gettempdir(),
                            help="directory for storing temporary files")
        common.add_argument("--verbose", required=False, type=int, default=1, choices=[0, 1, 2],
                            help="Indicate verbose level (0 for warning and errors only, 1 for info, 2 for debug)")
        common.add_argument("--log", required=False, type=checkLog, default="stdout", help="log output file")
        common.add_argument("-d", "--disable_prog_bar", required=False, action="store_true",
                            help="disables the progress bars")
        common.add_argument("-c", "--cpu", required=False, default=1, type=int, help="Number of available cpus")
        common.add_argument('-f', '--force', action="store_true",
                            help="Force writing in output directory and in pangenome output file.")
        sub._action_groups.append(common)
        if len(sys.argv) == 2 and sub.prog.split()[1] == sys.argv[1]:
            sub.print_help()
            exit(1)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    args = parser.parse_args()
    return args

def main():
    args = cmdLine()

    level = logging.INFO  # info, warnings and errors, default verbose == 1
    if hasattr(args, "verbose"):
        if args.verbose == 2:
            level = logging.DEBUG  # info, debug, warnings and errors
        elif args.verbose == 0:
            level = logging.WARNING  # only warnings and errors

        if args.log != sys.stdout and not args.disable_prog_bar:  # if output is not to stdout we remove progress bars.
            args.disable_prog_bar = True

        logging.basicConfig(stream=args.log, level=level,
                            format='%(asctime)s %(filename)s:l%(lineno)d %(levelname)s\t%(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
        logging.getLogger().info("Command: " + " ".join([arg for arg in sys.argv]))
        logging.getLogger().info("HDF5_2_JSON" + pkg_resources.get_distribution("hdf5_2_json").version)
    if args.subcommand == "write":
        hdf5_2_json.formats.launchFlat(args)

if __name__ == "__main__":
    main()
