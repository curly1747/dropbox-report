from module.app import DropBoxApp
import os
import configparser
import sys
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--output_name", type=str, default=f'report_{datetime.now():%Y-%m-%d %H-%M-%S}',
                    help=(f"The output will be generated with this config name in the '/output' folder. "
                          "If unset, default value is 'report_YYYY-MM-DD HH-MM-SS'"))
# parser.add_argument("-p", "--path", type=str, default='/',
#                     help=f"The path to get the report, must beginning with /. "
#                          "If unset, default value is '/', everything in the admin space will be captured.")
parser.add_argument("-l", "--max_level", type=int, default=1,
                    help=f"The sub-folder levels to be export to output file. "
                         f"If unset, all sub-levels will be export to output")

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

        # app.report_path(output_name=args.output_name, path=args.path, max_level=args.max_level)

        app.report(output_name=args.output_name, max_level=args.max_level)




    except KeyboardInterrupt:
        app.output_file.close()
        os._exit(0)
