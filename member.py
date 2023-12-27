from module.app import DropBoxApp
import os
import configparser
import argparse
from datetime import datetime
from prettytable import PrettyTable

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--output_name", type=str, default=f'report_{datetime.now():%Y-%m-%d %H-%M-%S}',
                    help=(f"The output will be generated with this config name in the '/output' folder. "
                          "If unset, default value is 'report_YYYY-MM-DD HH-MM-SS'"))
parser.add_argument("-m", "--member", type=str, default='',
                    help=(f"The identification of selected member (name or email). "
                          "If set, get the report of folder type (Shared or Private), path and size. "
                          "If unset, get the report of Shared and Private Folder count by each member in team.'"))
parser.add_argument("-l", "--max_level", type=int, default=999,
                    help=f"(Only supported for member-specified report) "
                         f"The sub-folder levels to be export to output file. "
                         f"If unset, all sub-levels will be export to output")
parser.add_argument("-r", "--skip_not_root", type=int, default=0,
                    help=f"Fetch all sub-folders, sub-files or just root?"
                         f"If set to 1, just get content of root (folder level 0). "
                         f"If unset or set to 0, get all sub-files and sub folders")

args = parser.parse_args()

if __name__ == "__main__":
    try:

        config = configparser.ConfigParser()
        config.read("config.ini")

        app = DropBoxApp(
            team_access=True,
            app_key=config.get('DROPBOX', 'app_key'),
            app_secret=config.get('DROPBOX', 'app_secret')
        )
        if args.member:
            app.member_report(output_name=args.output_name, member_indentify=args.member, max_level=args.max_level,
                              skip_not_root=args.skip_not_root)
        else:
            app.all_member_report(output_name=args.output_name)



    except KeyboardInterrupt:
        app.output_file.close()
        os._exit(0)
