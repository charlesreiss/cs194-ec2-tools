#!/usr/bin/python
import argparse
import logging
import sys
import codecs

import account_util

## startstop_instances.py --mode={start,stop} --ami=ami --type=type
def setup_args(parser):
    parser.add_argument('--mode', choices=['start','stop','untag','retag','terminate'], required=True)
    parser.add_argument('--ami', default='ami-b5a7ea85') # US West Oregon, HVM, 64-bit, Amazon Linux AMI
    parser.add_argument('--type', default='t2.micro')

def start_instances(args):
    user_list = account_util.get_users(args)
    ec2 = account_util.connect_ec2(args)
    existing_instances = account_util.instances_by_user(args, ec2)
    started_instances = {}
    for user in user_list:
        if user not in existing_instances:
            started_instances[user] = account_util.make_reservation_for(args, ec2, user, {
                'image_id': args.ami,
                'instance_type': args.type,
            })
    failed_instances = account_util.wait_for_and_tag_instances(args, ec2, started_instances)
    started_instances.update(existing_instances)
    for k, v in account_util.healthcheck_instances(args, ec2, started_instances).iteritems():
        failed_instances[k] = failed_instances.get(k, []) + v
    for user, instances in failed_instances.iteritems():
        for instance in instances:
            try:
                instance.terminate()
            except boto.exception.EC2REsponseError, e:
                logging.error('error terminating instance %s: %s', instance.id, e)
    return failed_instances.keys()

def stop_instances(args):
    ec2 = account_util.connect_ec2(args)
    for user, instances in account_util.instances_by_user(args, ec2, user_set=user_set).iteritems():
        if user not in user_set:
            continue
        for instance in instances:
            instance.stop()

def terminate_instances(args):
    user_set = set(account_util.get_users(args))
    ec2 = account_util.connect_ec2(args)
    for user, instances in account_util.instances_by_user(args, ec2, user_set=user_set).iteritems():
        for instance in instances:
            instance.terminate()

def untag_instances(args):
    user_set = set(account_util.get_users(args))
    ec2 = account_util.connect_ec2(args) 
    account_util.untag_instances(args, ec2, account_util.instances_by_user(args, ec2, user_set=user_set))

def retag_instances(args):
    user_set = set(account_util.get_users(args))
    ec2 = account_util.connect_ec2(args) 
    account_util.retag_instances(args, ec2, account_util.instances_by_user(args, ec2, user_set=user_set))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    setup_args(parser)
    account_util.setup_args(parser)
    args = parser.parse_args()
    account_util.init_logging(args)

    if args.mode == 'start':
        if args.ami is None:
            logging.fatal('Need to specify AMI')
            sys.exit(1)
        failed = start_instances(args)
        if len(failed):
            logging.error('Failed to start instances for %s', sorted(failed))
    elif args.mode == 'untag':
        untag_instances(args)
    elif args.mode == 'retag':
        retag_instances(args)
    elif args.mode == 'stop':
        stop_instances(args)
    elif args.mode == 'terminate':
        terminate_instances(args)

