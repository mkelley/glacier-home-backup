# Glacier backup
Backup home data to Amazon's Glacier.

    usage: glacier-backup.py [-h] [-b B] [--config] [--db] [--debug] [--checksum]
                             [directory_or_file]
    
    Home backup to Amazon Glacier.
    
    positional arguments:
      directory_or_file  Directory to be backed up, or file name for --checksum.
    
    optional arguments:
      -h, --help         show this help message and exit
      -b B               Chunk size for archive uploading in mebibytes (MiB).
      --config           Configure glacier-backup.
      --db               Show the database contents.
      --debug            Do not send anything to AWS, no backup database updates.
      --checksum         Print the SHA256 tree checksum of the given file.

## Features
* Uploads entire directories as single, encrypted, uncompressed tar files to Glacier, splitting into chunks if larger than a user-defined size.
* Keeps a database of uploaded archives and only uploads new archives after a minimum amount of time has elapsed.

## Caution
This tool has not been extensively tested.  I hope you find `glacier-backup` useful, but use at your own risk.  If you encounter errors, feedback is appreciated.

## Requirements
* Amazon's [AWS command-line interface](https://aws.amazon.com/cli/) set up with a defined profile.
* A functioning [AWS Glacier](https://aws.amazon.com/glacier/) vault.
* GnuPG set up with your prefered key for encryption.
* A GNU/Linux system: adaptations can probably be made for other systems.

## Examples
Edit the config section at the top of the file before using.

Backup a directory, uploading 8 MiB chunks at a time to Glacier:

    $ glacier-backup.py -b8 /var/backup/notwo/disks/data0/documents/alaska

A database file is created, recording the time of the upload and the response from AWS (archiveId, checksum, and location).  View the database with:

    $ glacier-backup.py --db
    {'glacier metadata': {'/var/backup/notwo/disks/data0/documents/alaska': {'archiveId': '...',
                                                                         'checksum': '...',
                                                                         'location': '...'}},
     'last backup': {'/var/backup/notwo/disks/data0/documents/alaska': datetime.datetime(2017, 1, 1, 16, 55, 25, 96642)}}

## Logging
Messages are printed to the console and sent to the system logger.  For rsyslogd users, create the following file to send to a dedicated log:

    # Begin /etc/rsyslog.d/99-backup.conf
    
    if $programname startswith 'glacier-backup' then /var/log/glacier-backup.log
    
    # End /etc/rsyslog.d/99-backup.conf

Then restart rsyslogd.