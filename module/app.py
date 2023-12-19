from dropbox import Dropbox, DropboxTeam, DropboxOAuth2FlowNoRedirect
from dropbox.files import FolderMetadata, FileMetadata, ListFolderResult, DeletedMetadata
from dropbox.team import TeamNamespacesListResult, NamespaceMetadata, NamespaceType
from dropbox.team import GroupsMembersListResult, MembersListResult, GroupMemberInfo, MemberProfile
from dropbox.team import TeamFolderListResult, TeamFolderMetadata, TeamFolderStatus
from dropbox.sharing import GroupMembershipInfo, SharedFolderMembers, GroupInfo, UserMembershipInfo, UserInfo, AccessLevel
from dropbox.exceptions import AuthError
from dropbox.users import FullAccount
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
from typing import Optional

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
    def __init__(self, obj: FolderMetadata = None, namespace='', level=0, type_=''):
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
        self.type = type_
        self.files = list()
        self.members = list()
        self.groups = list()
        self.total_file = 0
        self.total_folder = 0
        self.level = level
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

    def update(self, path, id_=None, parent=None, type_=None):
        self.name = path.split("/")[-1]
        self.path_display = path if path else '/'
        self.path_lower = path
        if id_:
            self.id = id_
        if parent:
            self.parent_id = parent.id
            self.parent = parent
        if type_:
            self.type = type_

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
        self.team_access = team_access
        self.dropbox = None
        self.dropbox_team = None
        self.dropbox_team_as_admin = None
        self.admin = None
        self.client = None
        self.type_mapping = {
            'team_folder': 'Team Folder',
            'app_folder': 'App Sandbox Folder',
            'other': 'Other Folder',
            'team_member_folder': 'Private Folder',
            'shared_folder': 'Shared Folder'
        }
        self.team_namespaces: list[NamespaceMetadata] = list()
        self.team_members: list[MemberProfile] = list()
        self.team_folders: list[TeamFolderMetadata] = list()
        self.remember_access_token = remember_access_token
        self.auto_refresh_access_token = auto_refresh_access_token
        self.config = configparser.ConfigParser()
        self.config.read('session.ini')
        self.access_token = self.config.get("SESSION", "ACCESS_TOKEN")
        self.refresh_token = self.config.get("SESSION", "REFRESH_TOKEN")
        self.output_name = None
        self.max_level = 9999
        self.root = Folder()
        self.backup = dict()
        self.result = list()
        self.total_folder = 0
        self.status = "PROCESSING"
        self.live_process = LiveProcess(app=self)
        self.folders = dict()
        self.wb = self.ws = self.output_file = self.output_writer = None
        self.auth()

    def update_backup(self, folder: Folder, write=True):
        parent_id = folder.parent.id if folder.parent else None
        last_modified = f'{folder.last_modified:%m/%d/%y %H:%M:%S}' if folder.last_modified else None,
        created_at = f'{folder.created_at:%m/%d/%y %H:%M:%S}' if folder.created_at else None,
        self.backup[folder.id] = {
            'parent_id': parent_id,
            'type': folder.type,
            'level': folder.level,
            'name': folder.name,
            'path_display': folder.path_display,
            'path_lower': folder.path_lower,
            'members': folder.members,
            'groups': folder.groups,
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

    def prepare_client(self):
        self.dropbox = Dropbox(
            oauth2_access_token=self.access_token,
            oauth2_refresh_token=self.refresh_token,
            app_key=self.app_key
        )
        self.client = self.dropbox
        if self.team_access:
            self.dropbox_team = DropboxTeam(
                oauth2_access_token=self.access_token,
                oauth2_refresh_token=self.refresh_token,
                app_key=self.app_key
            )
            self.admin = self.dropbox_team.team_token_get_authenticated_admin().admin_profile
            self.dropbox_team_as_admin = self.dropbox_team.as_admin(self.admin.team_member_id)
            self.client = self.dropbox_team_as_admin

    def auth(self, retry=False):

        if self.access_token and self.refresh_token:
            self.prepare_client()
            try:
                self.dropbox.check_and_refresh_access_token()
            except AuthError:
                self.access_token = ''
                self.refresh_token = ''
                self.update_session()
                self.auth(retry=True)

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

            self.prepare_client()

            if self.remember_access_token:
                self.update_session()

        current_account = self.client.users_get_current_account()

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
            folder.type,
            str(folder.level),
            folder.path_lower,
            self.sizeof_fmt(folder.size),
            f'{folder.sub_folder_recursive:,}',
            f'{folder.sub_folder_non_recursive:,}',
            created_at,
            last_modified,
            f'{folder.total_file:,}',
            str(len(folder.members)),
            str(len(folder.groups)),
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
        table.add_column("Type")
        table.add_column("Level")
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
            (type_, level, path, size, sub_folder_r, sub_folder_non_r,
             created_at, last_modified, files, members, groups, exec_time) = row
            table.add_row(type_, level, path, size, sub_folder_r, sub_folder_non_r,
                          created_at, last_modified, files, members, groups, exec_time)
        return table

    def report_path(self, output_name, path='', max_level=9999):
        path = '' if path == '/' else path
        self.max_level = max_level
        self.output_name = output_name
        self.root.update(path)
        self.check_backup()
        self.live_process.start()
        # account = self.dropbox.users_get_current_account()
        # self.get_path(folder=self.root, verify_id=account.account_id)
        self.get_path(folder=self.root)
        self.status = 'DONE'
        self.output_file.close()

    def report(self, output_name, max_level=9999):
        path = ''
        self.max_level = max_level
        self.output_name = output_name
        self.root.update(path)
        self.check_backup()
        self.live_process.start()

        # 1. Get team folder meta data for mapping in namespace
        self.team_folders = self.get_team_folders()

        # 2. Get namespace from root and run report
        # TODO: Check if other tag has team_member_id?
        namespaces = self.get_namespaces(types=['team_folder', 'app_folder', 'other'])
        team_folder_test_count = 0
        archived_team_folder_test_count = 0
        app_folder_test_count = 0
        other_test_count = 0
        for namespace in namespaces:
            namespace_root = Folder(namespace=namespace.namespace_id)
            # Classify folder type by namespace tag, If tag is team folder -> check status is active or archived
            # TODO: Check verify tag
            # TODO: Check mapping namespace's name and team folder's name (Apply only for team_folder type)
            type_ = self.verify_namespace_tag(namespace)

            # TODO: Below is condition verify to run test in small-scale (1 time per folder type)
            if type_ == 'Team Folder':
                team_folder_test_count += 1
                if team_folder_test_count > 1:
                    continue
            if type_ == "Archived Team Folder":
                archived_team_folder_test_count += 1
                if archived_team_folder_test_count > 1:
                    continue
            if type_ == "App Sandbox Folder":
                app_folder_test_count += 1
                if app_folder_test_count > 1:
                    continue
            if type_ == "Other Folder":
                other_test_count += 1
                if other_test_count > 1:
                    continue

            namespace_root.update(path='', id_=f'ns:{namespace.namespace_id}', parent=self.root, type_=type_)
            client = self.dropbox_team.as_user(namespace.team_member_id)
            # TODO: Check namespace's root is report under root
            self.get_path(folder=namespace_root, client=client)

        # 3. Get Team Member's Personal Space (Private Folder)
        self.team_members = self.get_team_member()
        for team_member in self.team_members:
            team_member_root = Folder()
            type_ = "Private Folder"
            # Issue: Dropbox currently only return name and path in DeletedMetadata of deleted files and folders.
            team_member_root.update(path='', id_=f'tm:{team_member.team_member_id}', parent=self.root, type_=type_)
            client = self.dropbox_team.as_user(team_member.team_member_id)
            # TODO: Check if only report content that owned by this user (avoid duplicate)
            self.get_path(folder=team_member_root, client=client, verify_id=team_member.account_id)
            # TODO: Below line is handle to run test in small-scale (1 time per team member's personal space)
            break

        self.status = 'DONE'
        self.output_file.close()

    def verify_namespace_tag(self, namespace: NamespaceMetadata):
        type_ = self.type_mapping[namespace.namespace_type._tag]
        if type_ == 'Team Folder':
            for team_folder in self.team_folders:
                if team_folder.name == namespace.name:
                    status: TeamFolderStatus = team_folder.status
                    if status.is_archived() or status.is_archive_in_progress():
                        type_ = "Archived Team Folder"
                    break
        return type_

    def get_team_folders(self) -> list[TeamFolderMetadata]:
        result = list()
        r: TeamFolderListResult = self.dropbox_team.team_team_folder_list()
        result.extend(r.team_folders)
        while r.has_more:
            r: TeamFolderListResult = self.dropbox_team.team_team_folder_list_continue(cursor=r.cursor)
            result.extend(r.team_folders)
        return result

    def get_namespaces(self, types=None) -> list[NamespaceMetadata]:
        result = list()
        types = ['team_folder', 'app_folder', 'other'] if not types else types
        r: TeamNamespacesListResult = self.dropbox_team.team_namespaces_list()
        namespace: NamespaceMetadata
        for namespace in r.namespaces:
            namespace_type: NamespaceType = namespace.namespace_type
            if 'team_folder' in types and namespace_type.is_team_folder():
                self.result.append(namespace)
            elif 'shared_folder' in types and namespace_type.is_shared_folder():
                self.result.append(namespace)
            elif 'private_folder' in types and namespace_type.is_team_member_folder():
                self.result.append(namespace)
            elif 'app_folder' in types and namespace_type.is_app_folder():
                self.result.append(namespace)
            elif 'other' in types and namespace_type.is_other():
                self.result.append(namespace)
        return result

    def update_output(self, folder):
        if folder.level <= self.max_level:
            self.output_writer.writerow([
                folder.type,
                folder.level,
                folder.path_display,
                folder.size,
                folder.sub_folder_non_recursive,
                folder.sub_folder_recursive,
                f"{folder.created_at:%Y-%m-%d}",
                f"{folder.last_modified:%Y-%m-%d}",
                folder.total_file,
                ', '.join(folder.members),
                ', '.join(folder.groups)
            ])

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

        if backup_file:
            os.remove(f'session/{self.output_name}.json')
        if result_file:
            os.remove(f'output/{self.output_name}.csv')

        self.prepare_output_file()

        self.output_writer.writerow([
            'Level', 'Path', 'Size (byte)',
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

    def get_files_list_folder(self):
        contents = self.client.files_list_folder(path="", recursive=False)
        for content in contents.entries:
            print(content.path_display)
        while True:
            if contents.has_more:
                contents = self.client.files_list_folder_continue(cursor=contents.cursor)
                for content in contents.entries:
                    print(content.path_display)
            else:
                exit()

    def get_team_member(self) -> list[MemberProfile]:
        result = list()
        contents: MembersListResult = self.dropbox_team.team_members_list()
        member: GroupMemberInfo
        for member in contents.members:
            profile: MemberProfile = member.profile
            result.append(profile.team_member_id)
        while contents.has_more:
            contents: MembersListResult = self.client.team_members_list_continue(cursor=contents.cursor)
            member: GroupMemberInfo
            for member in contents.members:
                profile: MemberProfile = member.profile
                result.append(profile)
        return result

    def get_group_members(self, group_id) -> [MemberProfile.email]:

        result: [MemberProfile.email] = list()
        from dropbox.team import GroupSelector
        contents: GroupsMembersListResult = self.client.team_groups_members_list(group=GroupSelector.group_id(group_id))
        member: GroupMemberInfo
        for member in contents.members:
            profile: MemberProfile = member.profile
            result.append(profile.email)
        while contents.has_more:
            contents: GroupsMembersListResult = self.client.team_groups_members_list_continue(
                cursor=contents.cursor)
            for member in contents.members:
                profile: MemberProfile = member.profile
                result.append(profile.email)

        return result

    @staticmethod
    def file_get_folder_by_client(client):
        contents: ListFolderResult = client.files_list_folder(path='')
        for content in contents.entries:
            if isinstance(content, FolderMetadata):
                content: FolderMetadata
                print(content.path_display)
        while contents.has_more:
            contents: ListFolderResult = client.files_list_folder_continue(cursor=contents.cursor)
            for content in contents.entries:
                if isinstance(content, FolderMetadata):
                    content: FolderMetadata
                    print(content.path_display)

    def get_member_space(self, member_id):
        client = self.dropbox_team.as_user(member_id)
        self.file_get_folder_by_client(client)

    @staticmethod
    def get_root_info(client):
        content: FullAccount = client.users_get_current_account()
        print(content.root_info)

    def get_path(self, folder=None, current_level=1, client=None, cursor=None, verify_id=None):
        if folder.id in self.folders:
            if self.folders[folder.id].status == "DONE":
                return self.folders[folder.id], True

        if not client:
            client = self.client
        if not cursor:
            contents: ListFolderResult = client.files_list_folder(path=folder.path_lower)
        else:
            contents: ListFolderResult = client.files_list_folder_continue(cursor=cursor)

        for content in contents.entries:
            if isinstance(content, FolderMetadata):
                # TODO: The line below is condition verify to run test in small-scale, remove to run in recursive mode
                if current_level <= self.max_level:
                    content: FolderMetadata
                    # Child folder will be inherited folder type from the parent
                    new_folder = Folder(obj=content, namespace=folder.namespace, level=current_level, type_=folder.type)
                    new_folder.parent = folder
                    self.update_backup(new_folder)

                    # If have id need to verify, is_owner will be set to False by default
                    is_owner = False if verify_id else True

                    # In case folder didn't sharing info, this folder is owned by this user
                    if not content.shared_folder_id:
                        is_owner = True
                    else:
                        # But if the parent is Member's Personal Space, may child folder is shared folder, verify it now!
                        # TODO: Check if Shared Folder only apply for Top-Level Folder of Private Content
                        if current_level == 1 and folder.type == 'Private Folder':
                            new_folder.type = "Shared Folder"
                        r: SharedFolderMembers = client.sharing_list_folder_members(
                            shared_folder_id=content.shared_folder_id)

                        # Verify if this user is the folder's owner
                        if verify_id:
                            member: UserMembershipInfo
                            for member in r.users:
                                member_info: UserInfo = member.user
                                member_access: AccessLevel = member.access_type
                                if member_info.account_id == verify_id and member_access.is_owner():
                                    is_owner = True
                                    break

                        if is_owner:
                            for member in r.users:
                                new_folder.members.append(f'({member.access_type._tag[0].upper()}) {member.user.email}')
                            group: GroupMembershipInfo
                            for group in r.groups:
                                group_info: GroupInfo = group.group
                                group_members = self.get_group_members(group_id=group_info.group_id)
                                group_output = (f'({group.access_type._tag[0].upper()}) '
                                                f'{group_info.group_name}({", ".join(group_members)})')
                                new_folder.groups.append(group_output)

                    # Only get report if this user is the folder's owner
                    if is_owner:
                        new_folder, is_backup = self.get_path(folder=new_folder, current_level=current_level + 1)
                        if not is_backup:
                            folder.add_folder(new_folder)
            if isinstance(content, FileMetadata):
                content: FileMetadata
                revisions = client.files_list_revisions(path=content.path_lower).entries
                new_file = File(
                    content, last_modified=revisions[0].server_modified, created_at=revisions[-1].server_modified
                )
                folder.add_file(new_file)
        if contents.has_more:
            return self.get_path(folder=folder, current_level=current_level, client=client, cursor=contents.cursor)
        else:
            self.record(folder)
        return folder, False
