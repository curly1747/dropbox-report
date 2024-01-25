from module.app import DropBoxApp
import os
import configparser
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--output_name", type=str, default=f'report_{datetime.now():%Y-%m-%d %H-%M-%S}',
                    help=(f"The output will be generated with this config name in the '/output' folder. "
                          "If unset, default value is 'report_YYYY-MM-DD HH-MM-SS'"))
parser.add_argument("-l", "--max_level", type=int, default=1,
                    help=f"The sub-folder levels to be export to output file. "
                         f"If unset, all sub-levels will be export to output")
parser.add_argument("-m", "--run_member_space", action='store_true',
                    help=f"If set, running in team's member spaces")

parser.add_argument("-t", "--run_team_space", action='store_true',
                    help=f"If set, running in team's spaces")

parser.add_argument("-o", "--run_other_space", action='store_true',
                    help=f"If set, running in team's other spaces")

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

        running_space = list()
        if args.run_member_space:
            running_space.append('member')
        if args.run_team_space:
            running_space.append('team')
        if args.run_other_space:
            running_space.append('other')

        if not running_space:
            print("Please setup running args")
            print("-m for team's member spaces, -t for team's spaces, -o for other spaces")
            print("Allow multiple spaces")
            exit(1)

        app.report_owner(output_name=args.output_name, max_level=args.max_level, running_space=running_space)

    except KeyboardInterrupt:
        app.output_file.close()
        os._exit(0)
