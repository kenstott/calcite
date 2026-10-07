#!/usr/bin/env python3
"""Creates or deletes the short-lived AWS resources of ephemeral-live-test.sh.

    ephemeral_aws.py create|delete <region> <name>

Uses the write-capable key in AWS_ADMIN_KEY / AWS_ADMIN_SECRET; the adapter's own key is
read-only. Everything is tagged application=<name>, and delete removes whatever carries it.
"""
import os
import sys

import boto3
from botocore.exceptions import ClientError


def client(service, region):
    return boto3.client(
        service,
        region_name=region,
        aws_access_key_id=os.environ["AWS_ADMIN_KEY"],
        aws_secret_access_key=os.environ["AWS_ADMIN_SECRET"],
    )


def tags(name):
    return [{"Key": "application", "Value": name}, {"Key": "Application", "Value": name},
            {"Key": "Name", "Value": name}]


def create(region, name):
    ec2 = client("ec2", region)
    vpc = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"][0]
    group = ec2.create_security_group(
        GroupName=name, Description="calcite cloud-ops test; safe to delete", VpcId=vpc["VpcId"],
        TagSpecifications=[{"ResourceType": "security-group", "Tags": tags(name)}])
    image = client("ssm", region).get_parameter(
        Name="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
    )["Parameter"]["Value"]
    instance = ec2.run_instances(
        ImageId=image, InstanceType="t3.micro", MinCount=1, MaxCount=1,
        SecurityGroupIds=[group["GroupId"]],
        InstanceInitiatedShutdownBehavior="terminate",
        TagSpecifications=[{"ResourceType": "instance", "Tags": tags(name)},
                           {"ResourceType": "volume", "Tags": tags(name)}],
    )["Instances"][0]
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance["InstanceId"]])
    client("ecr", region).create_repository(repositoryName=name, tags=tags(name))
    dynamodb = client("dynamodb", region)
    dynamodb.create_table(
        TableName=name, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}], Tags=tags(name))
    dynamodb.get_waiter("table_exists").wait(TableName=name)
    print(f"aws: created instance {instance['InstanceId']}, security group {group['GroupId']}, "
          f"ECR repository and DynamoDB table {name}")


def delete(region, name):
    ec2 = client("ec2", region)
    tag_filter = [{"Name": "tag:application", "Values": [name]}]
    instances = [
        i["InstanceId"]
        for r in ec2.describe_instances(
            Filters=tag_filter + [{"Name": "instance-state-name",
                                   "Values": ["pending", "running", "stopping", "stopped"]}]
        )["Reservations"]
        for i in r["Instances"]
    ]
    if instances:
        ec2.terminate_instances(InstanceIds=instances)
        ec2.get_waiter("instance_terminated").wait(InstanceIds=instances)
    for group in ec2.describe_security_groups(Filters=tag_filter)["SecurityGroups"]:
        ec2.delete_security_group(GroupId=group["GroupId"])
    for call in (lambda: client("ecr", region).delete_repository(repositoryName=name, force=True),
                 lambda: client("dynamodb", region).delete_table(TableName=name)):
        try:
            call()
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("RepositoryNotFoundException",
                                                   "ResourceNotFoundException"):
                raise
    left = ec2.describe_security_groups(Filters=tag_filter)["SecurityGroups"]
    print(f"aws: terminated {len(instances)} instance(s); tagged security groups left: {len(left)}")


if __name__ == "__main__":
    {"create": create, "delete": delete}[sys.argv[1]](sys.argv[2], sys.argv[3])
