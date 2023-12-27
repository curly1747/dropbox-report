from module.app import DropBoxApp
import os
import configparser
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--output_name", type=str, default=f'report_{datetime.now():%Y-%m-%d %H-%M-%S}',
                    help=(f"The output will be generated with this config name in the '/output' folder. "
                          "If unset, default value is 'report_YYYY-MM-DD HH-MM-SS'"))
parser.add_argument("-p", "--path", type=str, default='/',
                    help=(f"The path to get the report, must beginning with /. "
                          "If unset, default value is '/', "
                          "everything in the specific -m (member) or -t (team folder) will be captured.'"))
parser.add_argument("-m", "--member", type=str, default='',
                    help=f"The identification of selected member (name or email).")
parser.add_argument("-t", "--team_folder", type=str, default='',
                    help=f"The identification of selected team folder (name).")

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

        app.file_report(
            output_name=args.output_name, member_indentify=args.member,
            team_indentify=args.team_folder, path=args.path
        )



    except KeyboardInterrupt:
        app.output_file.close()
        os._exit(0)
