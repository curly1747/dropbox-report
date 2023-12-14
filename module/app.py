from dropbox import Dropbox, DropboxTeam, DropboxOAuth2FlowNoRedirect
from dropbox.files import FolderMetadata, FileMetadata
from dropbox.common import PathRoot
from dropbox.team import TeamNamespacesListResult, NamespaceMetadata
from dropbox.exceptions import AuthError
from threading import Thread
from rich.live import Live
from rich.table import Table
from rich.console import Console
from datetime import datetime
import webbrowser
import configparser
import time
import json
import os
import csv

console = Console()


class LiveProcess(Thread):
    def __init__(self, app):
        Thread.__init__(self)
        self.app = app

    def run(self):
        with Live(self.app.render_result(), refresh_per_second=1, screen=True) as live:
            while self.app.status == "PROCESSING":
                time.sleep(0.5)
                live.update(self.app.render_result())
        console.print(
            '[green]'
            f'Files: {self.app.root.total_file:,} | '
            f'Folders: {self.app.root.total_folder:,} | '
            f'Total Size: {self.app.sizeof_fmt(self.app.root.size)} | '
            f'Running Time: {self.app.sec_to_hours(int(time.time() - self.app.root.tic))}'
            '[/green]'
        )


class File:
    def __init__(self, obj: FileMetadata, last_modified, created_at):
        self.obj = obj
        self.id = obj.id
        self.size = obj.size
        self.last_modified = last_modified
        self.created_at = created_at


class Folder:
    def __init__(self, obj: FolderMetadata = None, namespace=''):
        self.tic = time.time()
        self.toc = None
        self.exec_time = 0
        self.obj = obj if obj else None
        self.id = obj.id if obj else "root"
        self.name = obj.name if obj else None
        self.path_display = obj.path_display if obj else None
        self.path_lower = obj.path_lower if obj else None
        self.namespace = namespace
        self.parent_id = None
        self.parent = None
        self.files = list()
        self.members = list()
        self.groups = list()
        self.total_file = 0
        self.total_folder = 0
        self.size = 0
        self.last_modified = None
        self.created_at = None
        self.sub_folder_non_recursive = 0
        self.sub_folder_recursive = 0
        self.status = "PROCESSING"

    def load_backup(self, backup, folder_id):
        self.id = folder_id
        self.parent_id = backup['parent_id']
        self.name = backup['name']
        self.path_display = backup['path_display']
        self.path_lower = backup['path_lower']
        self.members = backup['members']
        self.total_file = backup['total_file']
        self.total_folder = backup['total_folder']
        self.size = backup['size']
        self.last_modified = datetime.strptime(backup['last_modified'][0], '%m/%d/%y %H:%M:%S')
        self.created_at = datetime.strptime(backup['created_at'][0], '%m/%d/%y %H:%M:%S')
        self.sub_folder_non_recursive = backup['sub_folder_non_recursive']
        self.sub_folder_recursive = backup['sub_folder_recursive']
        self.status = backup['status']
        self.tic = backup['tic']
        self.toc = backup['toc']

    def update_path(self, path):
        self.name = path.split("/")[-1]
        self.path_display = path if path else '/'
        self.path_lower = path

    def done(self):
        self.status = "DONE"
        self.toc = time.time()
        self.exec_time = self.toc - self.tic

    def add_file(self, file: File):
        self.total_file += 1
        self.size += file.size
        if not self.last_modified or file.last_modified > self.last_modified:
            self.last_modified = file.last_modified
        if not self.created_at or file.created_at < self.created_at:
            self.created_at = file.created_at
        if self.parent:
            self.parent.add_file(file)

    def add_folder(self, folder, direct_parent=True):
        self.total_folder += 1
        if direct_parent:
            self.sub_folder_non_recursive += 1
        self.sub_folder_recursive += 1
        if self.parent:
            self.parent.add_folder(folder, direct_parent=False)


class DropBoxApp:
    def __init__(self, team_access=True, app_key=None, app_secret=None, remember_access_token=True,
                 auto_refresh_access_token=True):
        self.app_key = app_key
        self.app_secret = app_secret
        self.dropbox = None
        self.dropbox_team = None
        self.admin = None
        self.team_namespaces: [NamespaceMetadata] = list()
        self.team_members = list()
        self.remember_access_token = remember_access_token
        self.auto_refresh_access_token = auto_refresh_access_token
        self.config = configparser.ConfigParser()
        self.config.read('session.ini')
        self.access_token = self.config.get("SESSION", "ACCESS_TOKEN")
        self.refresh_token = self.config.get("SESSION", "REFRESH_TOKEN")
        self.output_name = None
        self.root = Folder()
        self.backup = dict()
        self.result = list()
        self.total_folder = 0
        self.status = "PROCESSING"
        self.live_process = LiveProcess(app=self)
        self.folders = dict()
        self.wb = self.ws = self.output_file = self.output_writer = None
        self.auth(team_access)

    def update_backup(self, folder: Folder, write=True):
        parent_id = folder.parent.id if folder.parent else None
        last_modified = f'{folder.last_modified:%m/%d/%y %H:%M:%S}' if folder.last_modified else None,
        created_at = f'{folder.created_at:%m/%d/%y %H:%M:%S}' if folder.created_at else None,
        self.backup[folder.id] = {
            'parent_id': parent_id,
            'name': folder.name,
            'path_display': folder.path_display,
            'path_lower': folder.path_lower,
            'members': folder.members,
            'total_file': folder.total_file,
            'total_folder': folder.total_folder,
            'size': folder.size,
            'last_modified': last_modified,
            'created_at': created_at,
            'sub_folder_non_recursive': folder.sub_folder_non_recursive,
            'sub_folder_recursive': folder.sub_folder_recursive,
            'status': folder.status,
            'tic': folder.tic,
            'toc': time.time()
        }
        if folder.parent:
            self.update_backup(folder.parent, write=False)
        if write:
            with open(f'session/{self.output_name}.json', 'w') as f:
                json.dump(self.backup, f)

    def prepare_client(self, team_access):
        self.dropbox_team = DropboxTeam(
            oauth2_access_token=self.access_token,
            oauth2_refresh_token=self.refresh_token,
            app_key=self.app_key
        )
        self.admin = self.dropbox_team.team_token_get_authenticated_admin().admin_profile
        # self.dropbox = self.dropbox_team.as_admin(self.admin.team_member_id)
        self.dropbox = Dropbox(
            oauth2_access_token=self.access_token,
            oauth2_refresh_token=self.refresh_token,
            app_key=self.app_key
        )

    def auth(self, team_access, retry=False):
        if self.access_token and self.refresh_token:
            self.prepare_client(team_access)
            try:
                self.dropbox.check_and_refresh_access_token()
            except AuthError:
                self.access_token = ''
                self.refresh_token = ''
                self.update_session()
                self.auth(team_access, retry=True)

        else:
            auth_flow = DropboxOAuth2FlowNoRedirect(self.app_key, use_pkce=True, token_access_type='offline')
            authorize_url = auth_flow.start()
            webbrowser.open(authorize_url, new=0, autoraise=True)
            print("1. Go to: " + authorize_url)
            print("2. Click \"Allow\" (you might have to log in first).")
            print("3. Copy the authorization code.")
            auth_code = input("Enter the authorization code here: ").strip()

            try:
                oauth_result = auth_flow.finish(auth_code)
            except Exception as e:
                print('Error: %s' % (e,))
                print(f'Please check your authorization code ({auth_code})')
                exit(1)

            self.access_token = oauth_result.access_token
            self.refresh_token = oauth_result.refresh_token

            self.prepare_client(team_access)

            if self.remember_access_token:
                self.update_session()

        current_account = self.dropbox.users_get_current_account()
        if not retry:
            console.print(
                "[green]"
                "Successfully set up client with account "
                f"[bold italic]{current_account.email}[/bold italic]"
                "[/green]"
            )

    def update_session(self):
        self.config.set('SESSION', 'ACCESS_TOKEN', self.access_token)
        self.config.set('SESSION', 'REFRESH_TOKEN', self.refresh_token)

        with open(f'session.ini', 'w') as configfile:
            self.config.write(configfile)

    def update_live_result(self, folder=None):
        self.total_folder += 1
        last_modified = f'{folder.last_modified:%m/%d/%Y}' if folder.last_modified else ""
        created_at = f'{folder.created_at:%m/%d/%Y}' if folder.created_at else ""
        self.result.append((
            folder.namespace,
            folder.path_lower,
            self.sizeof_fmt(folder.size),
            f'{folder.sub_folder_recursive:,}',
            f'{folder.sub_folder_non_recursive:,}',
            created_at,
            last_modified,
            f'{folder.total_file:,}',
            ','.join(folder.members),
            ','.join(folder.groups),
            f'{folder.exec_time:.1f}'
        ))

    def render_result(self):
        title = [
            f'Files: {self.root.total_file:,}',
            f'Folders: {self.root.total_folder:,}',
            f'Total Size: {self.sizeof_fmt(self.root.size)}',
            f'Running Time: {self.sec_to_hours(int(time.time() - self.root.tic))}',
        ]
        table = Table(title=' | '.join(title))
        table.add_column("NameSpace")
        table.add_column("Folder Path")
        table.add_column("Size")
        table.add_column("SubFolder (Recursive)")
        table.add_column("SubFolder (Non-Recursive)")
        table.add_column("Creation Date")
        table.add_column("Last Modified")
        table.add_column("Files")
        table.add_column("Members")
        table.add_column("Groups")
        table.add_column("Exec Time (s)")
        rows = list()
        for index, row in enumerate(reversed(self.result)):
            if index < 10:
                rows.append(row)
            else:
                table.add_row('...', '...', '...', '...', '...', '...', '...', '...')
                break
        for row in reversed(rows):
            (namespace, path, size, sub_folder_r, sub_folder_non_r,
             created_at, last_modified, files, members, groups, exec_time) = row
            table.add_row(namespace, path, size, sub_folder_r, sub_folder_non_r,
                          created_at, last_modified, files, members, groups, exec_time)
        return table

    def run(self, output_name, path=''):
        self.output_name = output_name
        self.root.update_path(path)
        self.check_backup()
        # self.wb = openpyxl.load_workbook(f'output/{self.output_name}.xlsx')
        # self.ws = self.wb.active
        self.live_process.start()
        self.get_path(self.dropbox, self.root)
        self.status = 'DONE'
        self.output_file.close()

    def report(self, output_name, path=''):
        self.output_name = output_name

        self.get_namespaces()
        self.check_backup()
        self.root.update_path('')
        # self.wb = openpyxl.load_workbook(f'output/{self.output_name}.xlsx')
        # self.ws = self.wb.active
        self.live_process.start()

        self.get_path(self.dropbox, self.root)


        # namespace: NamespaceMetadata
        # for namespace in self.team_namespaces:
        #     namespace_root = Folder(namespace=namespace.name)
        #     namespace_root.id = f'namespace_id:{namespace.namespace_id}'
        #     namespace_root.update_path(path)
        #     path_root = self.dropbox.with_path_root(PathRoot.root(namespace.namespace_id))
        #     self.get_path(path_root=path_root, folder=namespace_root)

    def get_namespaces(self):
        r: TeamNamespacesListResult = self.dropbox_team.team_namespaces_list()
        namespace: NamespaceMetadata
        for namespace in r.namespaces:
            if namespace.namespace_type.is_team_folder():
                self.team_namespaces.append(namespace)

    def update_output(self, folder):
        self.output_writer.writerow([
            folder.namespace,
            folder.path_display,
            folder.size,
            folder.sub_folder_non_recursive,
            folder.sub_folder_recursive,
            folder.created_at,
            folder.last_modified,
            folder.total_file,
            ','.join(folder.members),
            ','.join(folder.groups)
        ])
        # self.ws.append(new_row)
        # self.wb.save(f'output/{self.output_name}.xlsx')

    def record(self, folder: Folder):
        folder.done()
        self.update_output(folder)
        self.update_live_result(folder)
        self.update_backup(folder)

    def check_backup(self):

        backup_file = os.path.exists(f'session/{self.output_name}.json')
        result_file = os.path.exists(f'output/{self.output_name}.csv')
        if backup_file and result_file:
            backup = json.load(open(f'session/{self.output_name}.json'))
            resume = input("Backup file found "
                           f"({backup['root']['total_file']:,} files, {backup['root']['total_folder']:,} folders)"
                           ", would you like to resume? (Y/N): ").strip()
            if resume == 'Y':
                for folder_id in backup:
                    folder = Folder()
                    data = backup[folder_id]
                    if not (data['status'] == "PROCESSING" and data['size'] == 0):
                        folder.load_backup(backup=data, folder_id=folder_id)
                        self.folders[folder_id] = folder
                        if folder_id == "root":
                            self.total_folder = folder.total_folder
                            folder.tic = time.time() - (folder.toc - folder.tic)
                            self.root = folder
                for folder_id in self.folders:
                    if self.folders[folder_id].parent_id:
                        self.folders[folder_id].parent = self.folders[self.folders[folder_id].parent_id]
                for folder_id in self.folders:
                    self.update_backup(self.folders[folder_id], write=False)
                    if self.folders[folder_id].status == "DONE":
                        self.update_live_result(self.folders[folder_id])
                self.update_backup(self.root, write=False)
                self.prepare_output_file()
                return True

        if result_file:
            os.remove(f'session/{self.output_name}.json')
            os.remove(f'output/{self.output_name}.csv')
        self.prepare_output_file()
        self.output_writer.writerow([
            'Namespace', 'Path', 'Size (byte)',
            'subFolder (Non-Recursive)', 'subFolder (Recursive)',
            'Created Date', 'Last Modified', 'Files', 'Members', 'Groups'
        ])

    def prepare_output_file(self):
        self.output_file = open(f'output/{self.output_name}.csv', mode='a+', encoding='utf-8', newline='')
        self.output_writer = csv.writer(self.output_file)

    @staticmethod
    def sec_to_hours(seconds):
        h = (seconds // 3600)
        m = ((seconds % 3600) // 60)
        s = ((seconds % 3600) % 60)
        output = list()
        if h:
            output.append(f'{h} hours')
        if m:
            output.append(f'{m} mins')
        output.append(f'{s} secs')
        return ' '.join(output)

    @staticmethod
    def sizeof_fmt(num, suffix="B"):
        for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f}Yi{suffix}"

    def test(self):
        contents = self.dropbox.files_list_folder(path="", recursive=False)
        for content in contents.entries:
            print(content.path_display)
        while True:
            if contents.has_more:
                contents = self.dropbox.files_list_folder_continue(cursor=contents.cursor)
                for content in contents.entries:
                    print(content.path_display)
            else:
                exit()

    @staticmethod
    def get_files_list_folder(client):
        contents = client.files_list_folder(path="", recursive=False)
        for content in contents.entries:
            print(content.path_display)
        while True:
            if contents.has_more:
                contents = client.files_list_folder_continue(cursor=contents.cursor)
                for content in contents.entries:
                    print(content.path_display)
            else:
                exit()

    def get_team_member(self):
        client = self.dropbox_team
        # client = self.dropbox_team.as_admin(self.admin.team_member_id)

        contents = client.team_members_list()
        for member in contents.members:
            print(member)
        while True:
            if contents.has_more:
                contents = client.team_members_list_continue(cursor=contents.cursor)
                for member in contents.members:
                    print(member)
            else:
                exit()

    def get_member_space(self, member_id):
        client = self.dropbox_team.as_user(member_id)
        self.get_files_list_folder(client=client)

    def get_path(self, path_root, folder=None):
        if folder.id in self.folders:
            if self.folders[folder.id].status == "DONE":
                return self.folders[folder.id], True

        contents = path_root.files_list_folder(path=folder.path_lower)
        for content in contents.entries:
            if isinstance(content, FolderMetadata):
                new_folder = Folder(obj=content, namespace=folder.namespace)
                new_folder.parent = folder
                self.update_backup(new_folder)
                if content.shared_folder_id:
                    r = self.dropbox.sharing_list_folder_members(shared_folder_id=content.shared_folder_id)
                    for member in r.users:
                        new_folder.members.append(f'({member.access_type._tag[0].upper()}){member.user.email}')
                    for group in r.groups:
                        print(group)
                        # new_folder.groups.append(f'({group.access_type._tag[0].upper()}){group.group.group_name}')
                new_folder, is_backup = self.get_path(path_root, new_folder)
                if not is_backup:
                    folder.add_folder(new_folder)
            if isinstance(content, FileMetadata):
                revisions = self.dropbox.files_list_revisions(path=content.path_lower).entries
                new_file = File(
                    content, last_modified=revisions[0].server_modified, created_at=revisions[-1].server_modified
                )
                folder.add_file(new_file)
        self.record(folder)
        return folder, False
