#!/usr/bin/python3
import os
import sys
import argparse

class Config:
    pass

config = Config()
config.backup_cache = '/var/backup/glacier-cache/'
config.backupdb = config.backup_cache + 'backup.db'
config.min_age = 90  # days
# tar_options is a list of options, or an empty list
config.tar_options = ['-p', '--ignore-failed-read', '--exclude-tag=NOBACKUP']
config.gpg_key = 'Mike and Martha Backup Archive'
config.vault_name = 'home-backup'
config.aws_profile = 'corc'  # aws cli profile name

class CacheLocationExists(Exception):
    pass

class BackupTooYoung(Exception):
    pass

class TarGPGSplitError(Exception):
    pass

class AWSCLIError(Exception):
    pass

class Logger:
    """Activity and error logger.

    Parameters
    ----------
    backup_cache : string, optional
      The name of the directory in which to write the log.

    """
    
    def __init__(self, backup_cache=config.backup_cache):
        import logging
        assert isinstance(backup_cache, str)
        self.filename = '{}/backup.log'.format(backup_cache)
        logging.basicConfig(filename=self.filename, format='%(message)s',
                            level=logging.INFO)

        self('\n' + '=' * 72)
        self.timestamp()
        self('glacier-backup.py {}'.format(' '.join(sys.argv[1:])))

    def __call__(self, message):
        import logging
        print(message)
        logging.info(message)

    def timestamp(self):
        import logging
        from datetime import datetime
        logging.info('{}'.format(datetime.now().isoformat()))

class BackupDB:
    """Backup database IO.

    If no file exists, then an empty file is created.

    Parameters
    ----------
    log : Logger, optional
      The activity logger, or `None` for no logging.
    backupdb : string, optional
      The location and name of the backup database file.

    """

    backup_parameters = ('last backup', 'glacier metadata')
    
    def __init__(self, log=None, backupdb=config.backupdb):
        assert isinstance(log, (Logger, type(None)))
        assert isinstance(backupdb, str)
        self._log = log
        self.backupdb = backupdb
        
        if not os.path.exists(self.backupdb):
            self.log("Creating empty backup database.")

            db = {}
            for k in self.backup_parameters:
                db[k] = {}

            self.save(db)
    
    def log(self, msg):
        if self._log is not None:
            self._log(msg)
        else:
            print(msg)

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
        self.log('Updated database [{}]:\n  {}: {}'.format(
            parameter, directory, str(data)))

    def summary(self):
        """Summarize the backup database."""
        from pprint import pprint
        pprint(self.db())

class Glacier:
    """AWS Glacier interface.

    Parameters
    ----------
    log : Logger, optional
      Activity logger.
    aws_profile : string, optional
      The name of the AWS CLI profile to use.
    vault_name : string, optional
      The name of the Glacier vault.
    debug : bool, optional
      Set to `True` and commands will be printed by the shell, but not
      executed.

    """

    def __init__(self, log=None, aws_profile=config.aws_profile,
                 vault_name=config.vault_name, debug=False):
        assert isinstance(log, (Logger, type(None)))
        assert isinstance(aws_profile, str)
        assert isinstance(vault_name, str)
        assert isinstance(debug, bool)

        self._log = log
        self.aws_profile = aws_profile
        self.vault_name = vault_name
        self.debug = debug

        if self.debug:
            self.log('Glacier: Debug mode.  No AWS commands will be executed.')

    def log(self, msg):
        if self._log is not None:
            self._log(msg)
        else:
            print(msg)

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
        self.log('Glacier: ' + _cmd)
        
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

        import shlex
        
        assert isinstance(chunks, list)
        assert all([isinstance(chunk, str) for chunk in chunks])
        assert isinstance(chunk_size, int)
        assert (chunk_size % 1048576) == 0, "Chunk size must be in multiples of MiB."
        assert isinstance(description, str)
        
        if len(chunks) > 1:
            metadata = self._upload_multipart_archive(chunks, chunk_size, description)
        else:
            cs = self.checksum(chunks)[0]
            cmd = ('upload-archive'
                   ' --archive-description {}'
                   ' --body {}'
                   ' --checksum {}'
            ).format(shlex.quote(description), chunks[0], cs)
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
        self.log('Glacier: Total archive size: {}'.format(file_size))

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
    log : Logger
      The activity logger.
    backupdb : BackupDB
      The backup database.
    backup_cache : string, optional
      The backup cache location.
    tar_options : list, optional
      A list of options to include in the tar command.
    gpg_key : string, optional
      The name of the GPG key to use to encrypt the archive.
    min_age : int, optional
      The minimum Glacier archive age.  If the uploaded age is less
      than this (as recorced in the backup database), then a new
      archive will not be created.
    debug : bool, optional
      Do everything but interact with Glacier.
    cleanup : bool, optional
      After uploading archive, clean up the cache directory.
    
    """
    
    def __init__(self, chunk_size, log, backupdb,
                 backup_cache=config.backup_cache,
                 tar_options=config.tar_options, gpg_key=config.gpg_key,
                 min_age=config.min_age, aws_profile=config.aws_profile,
                 vault_name=config.vault_name, debug=False, cleanup=True):
        assert isinstance(chunk_size, int)
        assert isinstance(log, Logger)
        assert isinstance(backupdb, BackupDB)
        assert isinstance(backup_cache, str)
        assert isinstance(tar_options, list)
        assert isinstance(gpg_key, str)
        assert isinstance(aws_profile, str)
        assert isinstance(vault_name, str)
        assert isinstance(min_age, int)
        assert isinstance(debug, bool)
        assert isinstance(cleanup, bool)
        
        self.chunk_size = chunk_size
        self.log = log
        self.backupdb = backupdb
        self.backup_cache = backup_cache
        self.tar_options = tar_options
        self.gpg_key = gpg_key
        self.min_age = min_age
        self.aws_profile = aws_profile
        self.vault_name = vault_name
        self.debug = debug
        self.cleanup = cleanup

        if self.debug:
            self.log("Archiver: Debug mode.")

    def _clean_cache(self, directory, chunks):
        """Clean the cache for `directory`."""
        import shlex
        head, tail = os.path.split(directory)
        tail = tail.replace(' ', '_')
        cache = shlex.quote(self.backup_cache + tail)
        for chunk in chunks:
            os.remove(chunk)
        os.system('rmdir {}'.format(cache))
        self.log('Cleaned cache.\n')

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
        self.log("Archiver: Archiving {}".format(directory))

        dt = self.backupdb.last_backup_age(directory)
        if dt is None:
            self.log('Archiver: Directory not in backup database.')
        elif dt <= self.min_age:
            raise BackupTooYoung("Backup too young: age = {} days".format(dt))

        if dt is not None:
            # remember the last archiveId so that we can remove it after upload
            last_archiveId = self.backupdb.db()['glacier metadata'][directory]['archiveId']
        else:
            last_archiveId = None

        self.log('')
        self.log.timestamp()
        chunks, description, date = self._archive(directory)

        self.log.timestamp()
        glacier = Glacier(self.log, aws_profile=self.aws_profile,
                          vault_name=self.vault_name, debug=self.debug)
        metadata = glacier.upload_archive(chunks, self.chunk_size, description)
        if last_archiveId is not None:
            glacier.remove_archive(last_archiveId)

        self.log('')
        self.log.timestamp()
        if not self.debug:
            self.backupdb.update('last backup', directory, date)
            self.backupdb.update('glacier metadata', directory, metadata)

        if self.cleanup:
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
        cache = shlex.quote(self.backup_cache + tail)

        self.log('Archiver: Using {} for cache location.'.format(cache))

        # check that path is clean
        if os.path.exists(cache):
            raise CacheLocationExists("Cache location exists, remove before executing: {}".format(cache))

        now = datetime.now()
        description = "directory:{} date:{}".format(directory, now.isoformat())
        self.log('Archiver: ' + description)

        # create tar, encrypt, and split into chunks
        os.system('mkdir {}'.format(cache))
        prefix = '{}/archive-'.format(cache)

        cmd = ['tar'] + self.tar_options + ['-c', directory]
        self.log('Archiver: ' + ' '.join(cmd))
        tar = Popen(cmd, stdout=PIPE, stderr=PIPE)

        cmd = ['gpg', '-e', '-r', self.gpg_key]
        self.log('Archiver: ' + ' '.join(cmd))
        gpg = Popen(cmd, stdin=tar.stdout, stdout=PIPE, stderr=PIPE)

        cmd = ['split', '-a3', '-b{}'.format(self.chunk_size), '-', prefix]
        self.log('Archiver: ' + ' '.join(cmd))
        split = Popen(cmd, stdin=gpg.stdout, stderr=PIPE)

        status = 0
        output = []
        for prog in (tar, gpg, split):
            status = prog.wait()
            output.append(prog.stderr.read(-1))

        self.log('Archiver:')
        if status != 0:
            output.insert(0, 'Error creating archive with tar/gpg/split.')
            raise TarGPGSplitError('\n'.join(output))

        self.log('Archiver:')
        self.log(subprocess.check_output(['ls', '-l', cache]).decode())

        status, ls = subprocess.getstatusoutput('ls {}???'.format(prefix))
        if status != 0:
            raise TarGPGSplitError('No archive created.  Verify directory and read permissions.')
        chunks = ls.split()
        self.log("Archiver: {} chunks".format(len(chunks)))
        return chunks, description, now

parser = argparse.ArgumentParser(description='Home backup to Amazon Glacier.')
parser.add_argument('directory_or_file', nargs='?', default=None, help='Directory to be backed up, or file name for --checksum.')
parser.add_argument('-b', default=128, type=int, help='Chunk size for archive uploading in mebibytes (MiB).')
parser.add_argument('--db', action='store_true', help='Show the database contents.')
parser.add_argument('--log', action='store_true', help='Show the backup log.')
parser.add_argument('--debug', action='store_true', help='Do not send anything to AWS, no backup database updates.')
parser.add_argument('--no-cleanup', dest='cleanup', action='store_false', help='Do not clean up the cache location.  Useful with --debug for verifying archive creation.')
parser.add_argument('--checksum', action='store_true', help='Print the SHA256 tree checksum of the given file.')

args = parser.parse_args()
assert isinstance(args.b, int)

if args.db:
    BackupDB().summary()
    exit(0)
elif args.log:
    with open('{}/backup.log'.format(config.backup_cache), 'r') as inf:
        print(inf.read(-1))
    exit(0)
    
if args.directory_or_file is None:
    parser.error('Directory (or file for --checksum) is requried.')
    exit(1)

if args.checksum:
    assert os.path.isfile(args.directory_or_file), "{} is not a file.".format(args.directory_or_file)
    print(Glacier().checksum([args.directory_or_file])[0])
    exit(0)

log = Logger()
backupdb = BackupDB(log)
archiver = Archiver(args.b * 1024**2, log, backupdb, debug=args.debug,
                    cleanup=args.cleanup)

try:
    archiver.backup(args.directory_or_file)
    status = 0
except:
    e = sys.exc_info()
    log('\n{}: {}'.format(e[0].__name__, str(e[1])))
    status = 1
    if args.debug:
        import traceback
        traceback.print_exc()
    
log.timestamp()
exit(status)
