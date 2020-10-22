#!/usr/bin/env python

from cds_downloader import Downloader
import sys, getopt

def main(argv):
    try:
      opts, args = getopt.getopt(argv,"hm:o:k:",["mode=", "storage_path=","split_keys="])
    except getopt.GetoptError:
        print('ado_cds.py -m <mode> -o <storage_path> -k <split_keys>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('ado_cds.py -m <mode> -o <storage_path> -k <split_keys>')
            sys.exit()
        elif opt in ("-m", "--mode"):
            ado_mode = arg
        elif opt in ("-o", "--storage_path"):
            storage_path= arg
        elif opt in ("-s", "--split_keys"):
            split_keys = arg

if __name__ == "__main__":
    main(sys.argv[1:])
