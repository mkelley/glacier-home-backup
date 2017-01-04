# Glacier backup
Backup home data to Amazon's Glacier.

    usage: glacier-backup.py [-h] [-b B] [--db] [--log] [--debug] [--checksum]
                         [directory_or_file]
    
    Home backup to Amazon Glacier.
    
    positional arguments:
      directory_or_file  Directory to be backed up, or file name for --checksum.
    
    optional arguments:
      -h, --help         show this help message and exit
      -b B               Chunk size for archive uploading in mebibytes (MiB).
      --db               Show the database contents.
      --log              Show the backup log.
      --debug            Do not send anything to AWS, no backup database updates.
      --checksum         Print the SHA256 tree checksum of the given file.

## Features
* Uploads entire directories as single, encrypted, uncompressed tar files to Glacier, splitting into chunks if larger than a user-defined size.
* Keeps a database of uploaded archives and only uploads new archives after a minimum amount of time has elapsed.

## Caution
This tool has not been extensively tested.  I hope you find `glacier-backup` useful, but use at your own risk.  If you encounter errors, your feedback is appreciated.

## Requirements
* Amazon's [AWS command-line interface](https://aws.amazon.com/cli/) set up with a defined profile.
* A functioning [AWS Glacier](https://aws.amazon.com/glacier/) vault.
* GnuPG set up with your prefered key for encryption.
* The code is developed on a GNU/Linux system, so there may be a few places where adaptations are needed for other systems.

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

A log is also created to help debug automated backups.  View it:

    $ glacier-backup.py --log
    ========================================================================
    2017-01-01T16:55:25.079575
    glacier-backup.py -b8 /var/backup/notwo/disks/data0/documents/alaska
    Backup: /var/backup/notwo/disks/data0/documents/alaska
    Directory not in backup database.
    
    2017-01-01T16:55:25.082905
    Using /var/backup/glacier-cache/alaska for cache location.
    directory:/var/backup/notwo/disks/data0/documents/alaska date:2017-01-01T16:55:25.096642
    tar --exclude-tag=NOBACKUP -c /var/backup/notwo/disks/data0/documents/alaska | gpg -e -r 'Mike and Martha Backup Archive' | split -a3 -b8388608 - /var/backup/glacier-cache/alaska/archive-
    total 31648
    -rw-rw-r-- 1 msk msk 8388608 Jan  1 16:55 archive-aaa
    -rw-rw-r-- 1 msk msk 8388608 Jan  1 16:55 archive-aab
    -rw-rw-r-- 1 msk msk 8388608 Jan  1 16:55 archive-aac
    -rw-rw-r-- 1 msk msk 7237721 Jan  1 16:55 archive-aad
    
    4 chunks
    2017-01-01T16:55:27.851694
    Glacier: aws --profile corc glacier initiate-multipart-upload --archive-description 'directory:/var/backup/notwo/disks/data0/documents/alaska date:2017-01-01T16:55:25.096642' --part-size 8388608 --account-id - --vault-name examplevault --output json
    Total archive size: 32403545
    Glacier: aws --profile corc glacier upload-multipart-part --upload-id 2oc6S7AWcySS_RklcZhkCZq3IMnjwGL4Yl99PoP2JJ0Ip9ravgbNQGbiTqBXOg2yt3vv1O9PCD_pwvr5MgeT-mFExIra --body /var/backup/glacier-cache/alaska/archive-aaa --range "bytes 0-8388607/*" --checksum 08b14676c4fdd5a10d310bcbcd6132f2624922cae2415e505cdc6e77616accba --account-id - --vault-name examplevault --output json
    Glacier: aws --profile corc glacier upload-multipart-part --upload-id 2oc6S7AWcySS_RklcZhkCZq3IMnjwGL4Yl99PoP2JJ0Ip9ravgbNQGbiTqBXOg2yt3vv1O9PCD_pwvr5MgeT-mFExIra --body /var/backup/glacier-cache/alaska/archive-aab --range "bytes 8388608-16777215/*" --checksum 4632d9a04619a8f47157e6d471bf16d90905f3001b91efae7971aadf117c7b78 --account-id - --vault-name examplevault --output json
    Glacier: aws --profile corc glacier upload-multipart-part --upload-id 2oc6S7AWcySS_RklcZhkCZq3IMnjwGL4Yl99PoP2JJ0Ip9ravgbNQGbiTqBXOg2yt3vv1O9PCD_pwvr5MgeT-mFExIra --body /var/backup/glacier-cache/alaska/archive-aac --range "bytes 16777216-25165823/*" --checksum 48a97e15748d28ebd3b9cc3285b2910daec3f87935576b548b75ffcbdbf99995 --account-id - --vault-name examplevault --output json
    Glacier: aws --profile corc glacier upload-multipart-part --upload-id 2oc6S7AWcySS_RklcZhkCZq3IMnjwGL4Yl99PoP2JJ0Ip9ravgbNQGbiTqBXOg2yt3vv1O9PCD_pwvr5MgeT-mFExIra --body /var/backup/glacier-cache/alaska/archive-aad --range "bytes 25165824-32403544/*" --checksum 71f972f999f6a59de44ca9da84969e8822118fee60f61f2252b5f658cbd5bd6c --account-id - --vault-name examplevault --output json
    Glacier: aws --profile corc glacier complete-multipart-upload --upload-id 2oc6S7AWcySS_RklcZhkCZq3IMnjwGL4Yl99PoP2JJ0Ip9ravgbNQGbiTqBXOg2yt3vv1O9PCD_pwvr5MgeT-mFExIra --checksum 655af8c16d553a87db26f52019e1663ffcdb8d316bd0b8dd362a30799e467038 --archive-size 32403545 --account-id - --vault-name examplevault --output json
    
    2017-01-01T16:55:37.566587
    Updated database.
    Cleaned cache.
    
    2017-01-01T16:55:37.680591
