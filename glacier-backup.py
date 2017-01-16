#!/usr/bin/python3
import os
import sys
import argparse
import configparser
import logging
import logging.handlers

class CacheLocationExists(Exception):
    pass

class BackupTooYoung(Exception):
    pass

class TarGPGSplitError(Exception):
    pass

class AWSCLIError(Exception):
    pass

######################################################################
logger = logging.getLogger('glacier-backup')
logger.setLevel(logging.INFO)
# Jan 15 07:36:04 corc rsyslogd: [origin software="rsyslogd" swVersion="8.16.0" x-pid="822" x-info="http://www.rsyslog.com"] rsyslogd was HUPed
formatter = logging.Formatter('%(name)s: %(message)s')

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(formatter)
logger.addHandler(console)

syslog = logging.handlers.SysLogHandler(address='/dev/log')
syslog.setLevel(logging.INFO)
syslog.setFormatter(formatter)
logger.addHandler(syslog)

######################################################################
parser = argparse.ArgumentParser(description='Home backup to Amazon Glacier.')
parser.add_argument('directory_or_file', nargs='?', default=None, help='Directory to be backed up, or file name for --checksum.')
parser.add_argument('-b', default=128, type=int, help='Chunk size for archive uploading in mebibytes (MiB).')
parser.add_argument('--config', action='store_true', help='Configure Glacier.')
parser.add_argument('--db', action='store_true', help='Show the database contents.')
parser.add_argument('--debug', action='store_true', help='Do not send anything to AWS, no backup database updates.')
parser.add_argument('--checksum', action='store_true', help='Print the SHA256 tree checksum of the given file.')

args = parser.parse_args()
assert isinstance(args.b, int)

######################################################################
class Config:
    defaults = {
        'cache_dir': '/var/backup/glacier-cache/',
        'min_age': 90,
        'tar_options': '-p --ignore-failed-read --exclude-tag=NOBACKUP',
        'gpg_key_name': '',
        'vault_name': '',
        'aws_profile': ''
    }
    descriptions = {
        'cache_dir': 'Cache directory',
        'min_age': 'Minimum archive age in days',
        'tar_options': 'tar command options',
        'gpg_key_name': 'GPG key name',
        'vault_name': 'Glacier vault name',
        'aws_profile': 'AWS CLI profile name'
    }

    def __init__(self):
        self.logger = logging.getLogger('glacier-backup.Config')
        self.config_file = os.path.join(os.path.expanduser("~"),
                                        '.config', 'glacier-backup',
                                        'config.ini')
        self._config = {}
        for k, v in self.defaults.items():
            self[k] = v

        if not os.path.exists(self.config_file):
            logger.error('Configuration file not found.  Generating default file: {}'.format(self.config_file))
            logger.error('Edit configuration file, or run {} with --config option.'.format(sys.argv[0]))

            path = os.path.dirname(self.config_file)
            d = path.split(os.path.sep)
            for i in range(len(d)):
                x = os.path.sep.join(d[:i+1])
                if len(x) == 0:
                    continue
                if not os.path.exists(x):
                    os.mkdir(x)

            self.save()
            exit(1)
        else:
            self.load()

    def __setitem__(self, k, v):
        import subprocess
        
        if k not in self.defaults.keys():
            raise KeyError('Invalid config key name: {}'.format(k))
        
        if k == 'tar_options':
            if isinstance(v, list):
                assert all([isinstance(s, str) for s in v]), 'Each tar_options item must be a string.'
                self._config[k] = v
            elif isinstance(v, str):
                self._config[k] = v.split()
            else:
                raise ValueError('tar_options must be a string or list of strings.')
        elif k == 'min_age':
            self._config[k] = int(v)
        else:
            assert isinstance(v, str)
            self._config[k] = v

    def __getitem__(self, k):
        if k == 'backupdb':
            return os.path.join(self._config['cache_dir'], 'backup.db')
        else:
            return self._config[k]

    def _prompt(self, request, default):
        print('{} [{}]: '.format(request, default), end=' ', flush=True)
        answer = sys.stdin.readline().strip()
        answer = default if len(answer) == 0 else answer
        return answer

    def configure(self):
        for k, v in sorted(self._config.items()):
            if k == 'tar_options':
                self[k] = self._prompt(self.descriptions[k], ' '.join(v))
            else:
                self[k] = self._prompt(self.descriptions[k], v)
        self.save()

    def save(self):
        c = configparser.ConfigParser()
        c['default'] = dict(self._config,
                            tar_options=' '.join(self['tar_options']))
        with open(self.config_file, 'w') as outf:
            c.write(outf)

    def load(self):
        c = configparser.ConfigParser()
        c.read(self.config_file)
        for k in self._config.keys():
            self[k] = c['default'][k]

config = Config()
if args.config:
    config.configure()
    exit(0)

######################################################################
class BackupDB:
    """Backup database IO.

    If no file exists, then an empty file is created.

    Parameters
    ----------
    backupdb : string, optional
      The location and name of the backup database file.

    """

    backup_parameters = ('last backup', 'glacier metadata')
    
    def __init__(self, backupdb=config['backupdb']):
        assert isinstance(backupdb, str)
        self.backupdb = backupdb
        
        self.logger = logging.getLogger('glacier-backup.BackupDB')

        if not os.path.exists(self.backupdb):
            self.logger.info("Creating empty backup database.")

            db = {}
            for k in self.backup_parameters:
                db[k] = {}

            self.save(db)
    
    def save(self, db):
        """Overwrite backup database file with `db`."""
        import pickle
        with open(self.backupdb, 'bw') as outf:
            pickle.dump(db, outf)

    def db(self):
        """Return the backup database."""
        import pickle
        with open(self.backupdb, 'br') as inf:
            db = pickle.load(inf)
        return db

    def last_backup_age(self, directory):
        """Age of the last backup of `directory` in days, or `None`."""
        from datetime import datetime
        last_backup = self.db()['last backup']
        now = datetime.now()
        if directory in last_backup:
            dt = (now - last_backup[directory]).days
        else:
            dt = None
        return dt

    def update(self, parameter, directory, data):
        """Update backup database.

        Parameters
        ----------
        parameter : string
          The backup parameter, e.g., 'last backup'.
        directory : string
          The directory that was backed up.
        data : object
          The data to save.  Must be pickle-able.

        """
        from datetime import datetime
        db = self.db()
        assert isinstance(parameter, str)
        assert isinstance(directory, str)
        db[parameter][directory] = data
        self.save(db)
        self.logger.info('Updated database [{}]:\n  {}: {}'.format(
            parameter, directory, str(data)))

    def summary(self):
        """Summarize the backup database."""
        from pprint import pprint
        pprint(self.db())

class Glacier:
    """AWS Glacier interface.

    Parameters
    ----------
    aws_profile : string, optional
      The name of the AWS CLI profile to use.
    vault_name : string, optional
      The name of the Glacier vault.
    debug : bool, optional
      Set to `True` and commands will be printed by the shell, but not
      executed.

    """

    def __init__(self, aws_profile=config['aws_profile'],
                 vault_name=config['vault_name'], debug=False):
        assert isinstance(aws_profile, str)
        assert isinstance(vault_name, str)
        assert isinstance(debug, bool)

        self.logger = logging.getLogger('glacier-backup.Glacier')

        self.aws_profile = aws_profile
        self.vault_name = vault_name
        self.debug = debug

        if self.debug:
            self.logger.info('Debug mode.  No AWS commands will be executed.')

    def send(self, cmd):
        """Send Glacier command to AWS.

        Parameters
        ----------
        cmd : string
          The command to send, including options.

        Returns
        -------
        output : dictionary
          The output from AWS, if any.

        Raises
        ------
        AWSCLIError

        """

        import shlex
        import json
        import subprocess

        _cmd = 'aws --profile {} glacier {} --account-id - --vault-name {} --output json'.format(self.aws_profile, cmd, self.vault_name)
        self.logger.info(_cmd)
        
        if self.debug:
            return {'uploadId': '12345'}

        status, output = subprocess.getstatusoutput(_cmd)
        if status != 0:
            raise AWSCLIError('AWS CLI error: ' + output)

        if len(output) > 0:
            return json.loads(output)
        else:
            return {}

    def checksum(self, chunks):
        """File SHA256 tree checksum for AWS Glacier.

        Parameters
        ----------
        chunks : list of strings
          The names of the file chunks for which to generate a checksum.

        Returns
        -------
        file_hash : string
          The complete file hash.
        chunk_hashes : string
          The hash for each chunk.

        """

        import hashlib
        import binascii
        
        chunk_hashes = []
        for chunk in chunks:
            hashes = []
            with open(chunk, 'rb') as inf:
                while True:
                    b = inf.read(1048576)
                    if len(b) == 0:
                        eof = True
                        break

                    m = hashlib.sha256()
                    m.update(b)
                    hashes.append(m.digest())

            chunk_hashes.append(self._reduce_hashes(hashes))

        file_hash = binascii.hexlify(self._reduce_hashes(chunk_hashes)).decode()
        return file_hash, [binascii.hexlify(s).decode() for s in chunk_hashes]

    def _reduce_hashes(self, hashes):
        import hashlib
        reduced = []
        for a, b in zip(hashes[::2], hashes[1::2]):
            m = hashlib.sha256()
            m.update(a)
            m.update(b)
            reduced.append(m.digest())

        # check for odd number, include last hash, if needed.
        if len(hashes) % 2 == 1:
            reduced.append(hashes[-1])

        if len(reduced) == 1:
            # done!  pop it out of the list
            reduced = reduced[0]
        else:
            # recursion until complete
            while isinstance(reduced, list):
                reduced = self._reduce_hashes(reduced)

        return reduced

    def upload_archive(self, chunks, chunk_size, description):
        """Upload archive.

        Parameters
        ----------
        chunks : list of strings
          The names of the chunks to upload.
        chunk_size : int
          The chunk size in bytes.
        description : string
          The description of the archive to upload.

        Returns
        -------
        metadata : dictionary
          Output from Glacier: archiveID, checksum, and location.

        """

        assert isinstance(chunks, list)
        assert all([isinstance(chunk, str) for chunk in chunks])
        assert isinstance(chunk_size, int)
        assert (chunk_size % 1048576) == 0, "Chunk size must be in multiples of MiB."
        assert isinstance(description, str)
        
        if len(chunks) > 1:
            metadata = self._upload_multipart_archive(chunks, chunk_size, description)
        else:
            cs = self.checksum(chunks)[0]
            cmd = 'upload-archive --body {} --checksum {}'.format(
                chunks[0], cs)
            metadata = self.send(cmd)

        return metadata

    def _upload_multipart_archive(self, chunks, chunk_size, description):
        import shlex

        file_hash, chunk_hashes = self.checksum(chunks)
        
        # initiate upload
        cmd = (
            'initiate-multipart-upload'
            ' --archive-description {}'
            ' --part-size {}'
        ).format(shlex.quote(description), chunk_size)
        multipart = self.send(cmd)

        file_size = 0
        for chunk in chunks:
            file_size += os.stat(chunk).st_size
        self.logger.info('Total archive size: {}'.format(file_size))

        # loop through chunks, keeping track of combined file byte position
        next_byte = 0
        for i in range(len(chunks)):
            final_byte = min(next_byte + chunk_size - 1, file_size - 1)
            cmd = (
                'upload-multipart-part'
                ' --upload-id {}'
                ' --body {}'
                ' --range "bytes {}-{}/*"'
                ' --checksum {}'
            ).format(multipart['uploadId'], chunks[i],
                     next_byte, final_byte, chunk_hashes[i])

            try:
                self.send(cmd)
            except:
                # something went wrong, abort the upload
                cmd = 'abort-multipart-upload --upload-id {}'.format(
                    multipart['uploadId'])
                self.send(cmd)
                raise

            next_byte = final_byte + 1
            
        cmd = (
            'complete-multipart-upload'
            ' --upload-id {}'
            ' --checksum {}'
            ' --archive-size {}'
        ).format(multipart['uploadId'], file_hash, file_size)

        return self.send(cmd)

    def remove_archive(self, archiveId):
        """Remove an archive from Glacier.

        Parameters
        ----------
        archiveId : string
          The archive ID of the file to remove.

        """

        cmd = 'delete-archive --archive-id {}'.format(archiveId)
        self.send(cmd)

class Archiver:
    """Create archives, backup and retrieval.

    Parameters
    ----------
    chunk_size : int
      The file chunk size.
    backupdb : BackupDB
      The backup database.
    cache_dir : string, optional
      The backup cache location.
    tar_options : list, optional
      A list of options to include in the tar command.
    gpg_key_name : string, optional
      The name of the GPG key to use to encrypt the archive.
    min_age : int, optional
      The minimum Glacier archive age.  If the uploaded age is less
      than this (as recorced in the backup database), then a new
      archive will not be created.
    debug : bool, optional
      Do everything but interact with Glacier.
    
    """
    
    def __init__(self, chunk_size, backupdb, cache_dir=config['cache_dir'],
                 tar_options=config['tar_options'],
                 gpg_key_name=config['gpg_key_name'], min_age=config['min_age'],
                 aws_profile=config['aws_profile'],
                 vault_name=config['vault_name'], debug=False):
        assert isinstance(chunk_size, int)
        assert isinstance(backupdb, BackupDB)
        assert isinstance(cache_dir, str)
        assert isinstance(tar_options, list)
        assert isinstance(gpg_key_name, str)
        assert isinstance(aws_profile, str)
        assert isinstance(vault_name, str)
        assert isinstance(min_age, int)
        
        self.chunk_size = chunk_size
        self.backupdb = backupdb
        self.cache_dir = cache_dir
        self.tar_options = tar_options
        self.gpg_key_name = gpg_key_name
        self.min_age = min_age
        self.aws_profile = aws_profile
        self.vault_name = vault_name
        self.debug = debug

        self.logger = logging.getLogger('glacier-backup.Archiver')
        
        if self.debug:
            self.logger.info("Debug mode.")

    def _clean_cache(self, directory, chunks):
        """Clean the cache for `directory`."""
        import shlex
        head, tail = os.path.split(directory)
        tail = tail.replace(' ', '_')
        cache = shlex.quote(self.cache_dir + tail)
        for chunk in chunks:
            os.remove(chunk)
        os.system('rmdir {}'.format(cache))
        self.logger.info('Cleaned cache.')

    def backup(self, directory):
        """Tar, encrypt, and split a directory, and upload to Glacier.

        Parameters
        ----------
        directory : string, optional
          The directory to archive.

        Raises
        ------
        BackupTooYoung

        """

        assert isinstance(directory, str)
        assert os.path.isdir(directory)

        directory = os.path.normpath(directory)
        self.logger.info("Archiving {}".format(directory))

        dt = self.backupdb.last_backup_age(directory)
        if dt is None:
            self.logger.warning('Directory not in backup database.')
        elif dt <= self.min_age:
            raise BackupTooYoung("Backup too young: age = {} days".format(dt))

        if dt is not None:
            # remember the last archiveId so that we can remove it after upload
            last_archiveId = self.backupdb.db()['glacier metadata'][directory]['archiveId']
        else:
            last_archiveId = None

        chunks, description, date = self._archive(directory)

        glacier = Glacier(aws_profile=self.aws_profile,
                          vault_name=self.vault_name, debug=self.debug)
        metadata = glacier.upload_archive(chunks, self.chunk_size, description)
        if last_archiveId is not None:
            glacier.remove_archive(last_archiveId)

        if not self.debug:
            self.backupdb.update('last backup', directory, date)
            self.backupdb.update('glacier metadata', directory, metadata)
        self._clean_cache(directory, chunks)

    def _archive(self, directory):
        """Create an encrypted archive of a directory, split into chunks.

        Parameters
        ----------
        directory : string
          The name of the directory.

        Returns
        -------
        chunks : list of strings
          The names of the chunks.
        description : string
          The directory name and time.
        date : datetime
          The time of the archive creation.

        Raises
        ------
        CacheLocationExists
        TarGPGSplitError

        """

        import shlex
        import tarfile
        import subprocess
        from subprocess import Popen, PIPE
        from datetime import datetime    

        head, tail = os.path.split(directory)
        tail = tail.replace(' ', '_')
        cache = shlex.quote(self.cache_dir + tail)

        self.logger.info('Using {} for cache location.'.format(cache))

        # check that path is clean
        if os.path.exists(cache):
            raise CacheLocationExists("Cache location exists, remove before executing: {}".format(cache))

        now = datetime.now()
        description = "directory:{} date:{}".format(directory, now.isoformat())
        self.logger.info(description)

        # create tar, encrypt, and split into chunks
        os.system('mkdir {}'.format(cache))
        prefix = '{}/archive-'.format(cache)

        cmd = ['tar'] + self.tar_options + ['-c', directory]
        self.logger.info(' '.join(cmd))
        tar = Popen(cmd, stdout=PIPE, stderr=PIPE)

        cmd = ['gpg', '-e', '-r', self.gpg_key_name]
        self.logger.info(' '.join(cmd))
        gpg = Popen(cmd, stdin=tar.stdout, stdout=PIPE, stderr=PIPE)

        cmd = ['split', '-a3', '-b{}'.format(self.chunk_size), '-', prefix]
        self.logger.info(' '.join(cmd))
        split = Popen(cmd, stdin=gpg.stdout, stderr=PIPE)

        output = []
        for prog in (tar, gpg, split):
            r = prog.communicate()
            if r[1] is not None:
                output.append(r[1].decode())

        status = 0
        for prog in (tar, gpg, split):
            status += prog.returncode

        if status != 0:
            output.insert(0, 'Error creating archive with tar/gpg/split.')
            raise TarGPGSplitError('\n'.join(output))

        self.logger.info(subprocess.check_output(['ls', '-l', cache]).decode())

        status, ls = subprocess.getstatusoutput('ls {}???'.format(prefix))
        if status != 0:
            raise TarGPGSplitError('No archive created.  Verify directory and read permissions.')
        chunks = ls.split()
        self.logger.info("{} chunks".format(len(chunks)))
        return chunks, description, now
    
######################################################################
if args.directory_or_file is None:
    parser.error('Directory (or file for --checksum) is requried.')
    exit(1)

if args.checksum:
    assert os.path.isfile(args.directory_or_file), "{} is not a file.".format(args.directory_or_file)
    print(Glacier().checksum([args.directory_or_file])[0])
    exit(0)

logger.info('Command line parameters: {}'.format(' '.join(sys.argv[1:])))
backupdb = BackupDB()
archiver = Archiver(args.b * 1024**2, backupdb, debug=args.debug)

try:
    archiver.backup(args.directory_or_file)
    status = 0
except:
    e = sys.exc_info()
    logger.error('\n{}: {}'.format(e[0].__name__, str(e[1])))
    status = 1
    if args.debug:
        import traceback
        logger.debug(traceback.format_exc())
    
exit(status)
