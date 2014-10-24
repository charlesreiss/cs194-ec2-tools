#!/usr/bin/python
from __future__ import print_function
import argparse

import account_util

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    account_util.setup_args(parser)
    args = parser.parse_args()
    ec2 = account_util.connect_ec2(args)
    PATTERN = "%(user)12s %(instance)10s %(state)16s %(instance_type)10s %(tagged_p)1s"
    print(PATTERN % {'user': 'user', 'instance': 'ID', 'state': 'State', 'instance_type': 'Type',
                     'tagged_p': 'Active?'})
    real_instance_tags = ec2.get_all_tags({'resource_type': 'instance', 'key': 'for_user'})
    tagged_ids = set()
    for tag in real_instance_tags:
        tagged_ids.add(tag.res_id)
    for user, instances in account_util.instances_by_user(args, ec2).iteritems():
        for instance in instances:
            tagged_p = 'Y' if instance.id in tagged_ids else 'N'
            print(PATTERN % {'user': user, 'instance': instance.id, 'state': instance.state,
                             'instance_type': instance.instance_type, 'tagged_p': tagged_p })

