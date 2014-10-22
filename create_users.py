#!/usr/bin/python
from __future__ import print_function

import argparse
import codecs
import os
import os.path

import account_util

## TODO: create_users.py <number of users>
##       create user000 --> userXXX -- assign to students or groups/track?

## XXX: Need to distribute SSH keys somehow?
if __name__ == '__main__':
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--init_db', action='store_true', default=False)
    parser.add_argument('--wipe_first', action='store_true', default=False)
    parser.add_argument('--skip_create', action='store_true', default=False)
    account_util.setup_args(parser)
    args = parser.parse_args()
    account_util.init_logging(args)

    iam = account_util.connect_iam(args)
    ec2 = account_util.connect_ec2(args)
    dbh = account_util.connect_db(args)
    if args.init_db:
        account_util.init_db(args, dbh)
        if not os.path.exists(args.ssh_key_dir):
            os.makedirs(args.ssh_key_dir)

    if not args.skip_create:
        with codecs.open(args.users_from_list, 'r', 'utf-8') as fh:
            for line in fh:
                name, user_name, rest = line.split('\t', 2)
                if args.wipe_first:
                    account_util.wipe_account(args, dbh, iam, ec2, user_name)
                account_util.create_account(args, dbh, iam, ec2, user_name, name,
                    note='From %s' % (args.users_from_list))

    passwords = account_util.get_all_passwords(args, dbh)
    for user, password in passwords.iteritems():
        print("%-10s %40s" % (user, password))
    
    dbh.close()

