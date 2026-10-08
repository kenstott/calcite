#!/usr/bin/env python3
"""Creates, or deletes, what AWSRoleAssumptionLiveTest needs: a user who may do nothing
but assume one role, and that role with read-only access.

    ephemeral_aws_role.py create|delete

The administrator key comes from AWS_ADMIN_KEY and AWS_ADMIN_SECRET. `create` appends
three aws.assumeRole.* lines to cloud-ops/src/test/resources/local-test.properties, a
file git ignores; `delete` removes them. Nothing secret is printed.
"""
import json
import os
import pathlib
import sys

import boto3
from botocore.exceptions import ClientError

NAME = "calcite-cloudops-test-assume"
TAGS = [{"Key": "application", "Value": "calcite-cloudops-test"}]
READ_ONLY = "arn:aws:iam::aws:policy/ReadOnlyAccess"
PROPERTIES = (pathlib.Path(__file__).resolve().parents[1]
              / "src" / "test" / "resources" / "local-test.properties")
PREFIX = "aws.assumeRole."


def clients():
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ADMIN_KEY"],
        aws_secret_access_key=os.environ["AWS_ADMIN_SECRET"],
        region_name="us-east-1")
    return session.client("iam"), session.client("sts")


def without_settings():
    return [line for line in PROPERTIES.read_text().splitlines()
            if not line.startswith(PREFIX)]


def create():
    iam, sts = clients()
    account = sts.get_caller_identity()["Account"]
    user_arn = iam.create_user(UserName=NAME, Tags=TAGS)["User"]["Arn"]
    role_arn = "arn:aws:iam::%s:role/%s" % (account, NAME)
    iam.put_user_policy(UserName=NAME, PolicyName="assume-one-role", PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole", "Resource": role_arn}]}))
    # A user created a moment ago is not yet a valid principal for a trust policy
    iam.get_waiter("user_exists").wait(UserName=NAME)
    trust = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"AWS": user_arn},
                       "Action": "sts:AssumeRole"}]})
    for attempt in range(12):
        try:
            iam.create_role(RoleName=NAME, AssumeRolePolicyDocument=trust, Tags=TAGS)
            break
        except ClientError as error:
            if error.response["Error"]["Code"] != "MalformedPolicyDocument" or attempt == 11:
                raise
            import time
            time.sleep(5)
    iam.attach_role_policy(RoleName=NAME, PolicyArn=READ_ONLY)
    key = iam.create_access_key(UserName=NAME)["AccessKey"]
    lines = without_settings() + [
        PREFIX + "accessKeyId=" + key["AccessKeyId"],
        PREFIX + "secretAccessKey=" + key["SecretAccessKey"],
        PREFIX + "roleArn=" + role_arn]
    PROPERTIES.write_text("\n".join(lines) + "\n")
    print("created user and role %s in account %s" % (NAME, account))


def gone(call, **arguments):
    try:
        call(**arguments)
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchEntity":
            raise


def delete():
    iam, _ = clients()
    try:
        for key in iam.list_access_keys(UserName=NAME)["AccessKeyMetadata"]:
            iam.delete_access_key(UserName=NAME, AccessKeyId=key["AccessKeyId"])
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchEntity":
            raise
    gone(iam.delete_user_policy, UserName=NAME, PolicyName="assume-one-role")
    gone(iam.delete_user, UserName=NAME)
    gone(iam.detach_role_policy, RoleName=NAME, PolicyArn=READ_ONLY)
    gone(iam.delete_role, RoleName=NAME)
    PROPERTIES.write_text("\n".join(without_settings()) + "\n")
    left = [u["UserName"] for u in iam.list_users()["Users"] if u["UserName"] == NAME]
    left += [r["RoleName"] for r in iam.list_roles()["Roles"] if r["RoleName"] == NAME]
    if left:
        sys.exit("still present: %s" % left)
    print("deleted user and role %s" % NAME)


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("create", "delete"):
        sys.exit(__doc__)
    create() if sys.argv[1] == "create" else delete()
