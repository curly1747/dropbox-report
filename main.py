from module.app import DropBoxApp
import os
import configparser

if __name__ == "__main__":
    try:

        config = configparser.ConfigParser()
        config.read("config.ini")

        app = DropBoxApp(
            team_access=True,
            app_key=config.get('DROPBOX', 'app_key'),
            app_secret=config.get('DROPBOX', 'app_secret')
        )

        app.report(output_name='test-1', path='')

        # app = DropBoxApp(
        #     team_access=False,
        #     app_key=config.get('DROPBOX', 'app_key'),
        #     app_secret=config.get('DROPBOX', 'app_secret')
        # )
        # app.run(output_name='test-4', path='')
    except KeyboardInterrupt:
        app.output_file.close()
        os._exit(0)
