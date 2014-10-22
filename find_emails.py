#!/usr/bin/python
from __future__ import print_function

import argparse
import codecs
import logging
import pandas as pd
import re

def clean_name(name):
    return re.sub(r'[^a-zA-Z]', '', name).lower()

def get_last(row):
    name = row['Student Name']
    last = name.split(',')[0]
    return clean_name(last)

def get_first(row):
    name = row['Student Name']
    first = name.split(',')[1]
    first = first.split(' ')[0]
    return clean_name(first)

def lookup_student(name, students):
    first, last = name.split(' ', 1)
    mask = (students['Last'] == clean_name(last)) & (students['First'] == clean_name(first))
    if sum(mask) == 0:
        logging.fatal('Could not find student for %s', name)
        return None
    elif sum(mask) > 1:
        logging.fatal('Found multiple matches for %s:\n%s', name, students[mask])
        return None
    else:
        return students[mask]

def pd_to_string(item):
    return item.tolist()[0]

def make_group_line(group_row, students, seen_emails):
    emails = []
    for name in group_row[['student1','student2','student3']]:
        if name == '-':
            continue
        student = lookup_student(name, students)
        assert(len(student) > 0)
        email = pd_to_string(student['Email Address'])
        seen_emails.add(email)
        emails.append("{name} <{email}>".format(
            name=pd_to_string(student['Student Name']),
            email=email,
        ))
    return u"{group_name}\t{user_name}\t{email_list}".format(
        group_name=group_row['group'],
        user_name=group_row['user'],
        email_list=u"\t".join(emails)
    )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('groups')
    parser.add_argument('students')
    parser.add_argument('output')
    args = parser.parse_args()

    students = pd.read_csv(args.students, encoding='utf-8')
    students['Last'] = students.apply(get_last, axis=1)
    students['First'] = students.apply(get_first, axis=1)
    groups = pd.read_csv(args.groups, encoding='utf-8').fillna('-')

    seen_emails = set()
    lines = groups.apply(lambda row: make_group_line(row, students, seen_emails), axis=1)
    seen_email_mask = students.apply(lambda row: row['Email Address'] in seen_emails, axis=1)
    if sum(seen_email_mask) != len(students):
        logging.fatal('unseen_emails = %s', students[~seen_email_mask])
        assert(False)
    with codecs.open(args.output, 'w', 'utf-8') as fh:
        fh.write("\n".join(lines))
        fh.write("\n")

