#!/usr/bin/env python3
"""Creates or deletes the short-lived AWS resources of ephemeral-live-test.sh.

    ephemeral_aws.py create|delete <region> <name>

Uses the write-capable key in AWS_ADMIN_KEY / AWS_ADMIN_SECRET; the adapter's own key is
read-only. Everything is tagged application=<name> and named after <name>; delete removes
whatever carries the name, whether or not create finished.
"""
import json
import os
import secrets
import sys
import time

import boto3
from botocore.exceptions import ClientError, WaiterError

EKS_CLUSTER_POLICY = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
EKS_NODE_POLICIES = [
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
]
# The EKS control plane is not offered in this zone
EKS_EXCLUDED_ZONES = {"us-east-1e"}
SLOW = {"Delay": 20, "MaxAttempts": 90}  # up to 30 minutes


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


def tag_map(name):
    return {t["Key"]: t["Value"] for t in tags(name)}


def role(iam, role_name, service, policies, name):
    trust = {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]}
    # IAM tag keys are case-insensitive: "application" and "Application" cannot both be set
    arn = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust),
                          Tags=[{"Key": "application", "Value": name}])["Role"]["Arn"]
    for policy in policies:
        iam.attach_role_policy(RoleName=role_name, PolicyArn=policy)
    return arn


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
    client("ecr", region).create_repository(repositoryName=name, tags=tags(name))
    dynamodb = client("dynamodb", region)
    dynamodb.create_table(
        TableName=name, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}], Tags=tags(name))

    # The slow ones are started together and waited for afterwards
    password = secrets.token_urlsafe(24)  # never printed; the databases live for minutes
    rds = client("rds", region)
    rds.create_db_instance(
        DBInstanceIdentifier=name, Engine="postgres", DBInstanceClass="db.t3.micro",
        AllocatedStorage=20, MasterUsername="calcite", MasterUserPassword=password,
        BackupRetentionPeriod=0, PubliclyAccessible=False, StorageEncrypted=True,
        DeletionProtection=False, Tags=tags(name))
    # An Aurora cluster without instances costs nothing and still lists as a cluster
    rds.create_db_cluster(
        DBClusterIdentifier=name + "-aurora", Engine="aurora-postgresql",
        MasterUsername="calcite", MasterUserPassword=password, DeletionProtection=False,
        Tags=tags(name))
    elasticache = client("elasticache", region)
    elasticache.create_cache_cluster(
        CacheClusterId=name, Engine="redis", CacheNodeType="cache.t4g.micro", NumCacheNodes=1,
        Tags=tags(name))

    iam = client("iam", region)
    cluster_role = role(iam, name + "-eks-cluster", "eks.amazonaws.com", [EKS_CLUSTER_POLICY], name)
    node_role = role(iam, name + "-eks-node", "ec2.amazonaws.com", EKS_NODE_POLICIES, name)
    subnets = [
        s["SubnetId"]
        for s in ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc["VpcId"]]},
                                               {"Name": "default-for-az", "Values": ["true"]}]
                                      )["Subnets"]
        if s["AvailabilityZone"] not in EKS_EXCLUDED_ZONES
    ][:3]
    eks = client("eks", region)
    for attempt in range(12):  # a role just created is not visible to EKS at once
        try:
            eks.create_cluster(name=name, roleArn=cluster_role,
                               resourcesVpcConfig={"subnetIds": subnets}, tags=tag_map(name))
            break
        except ClientError as e:
            if e.response["Error"]["Code"] != "InvalidParameterException" or attempt == 11:
                raise
            time.sleep(10)
    eks.get_waiter("cluster_active").wait(name=name, WaiterConfig=SLOW)
    eks.create_nodegroup(
        clusterName=name, nodegroupName=name, nodeRole=node_role, subnets=subnets,
        instanceTypes=["t3.small"], scalingConfig={"minSize": 1, "maxSize": 1, "desiredSize": 1},
        tags=tag_map(name))
    eks.get_waiter("nodegroup_active").wait(clusterName=name, nodegroupName=name,
                                            WaiterConfig=SLOW)

    ec2.get_waiter("instance_running").wait(InstanceIds=[instance["InstanceId"]])
    dynamodb.get_waiter("table_exists").wait(TableName=name)
    rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=name, WaiterConfig=SLOW)
    rds.get_waiter("db_cluster_available").wait(DBClusterIdentifier=name + "-aurora",
                                                WaiterConfig=SLOW)
    elasticache.get_waiter("cache_cluster_available").wait(CacheClusterId=name,
                                                           WaiterConfig=SLOW)
    print(f"aws: created instance {instance['InstanceId']}, security group {group['GroupId']}, "
          f"ECR repository, DynamoDB table, RDS instance, Aurora cluster, ElastiCache cluster, "
          f"EKS cluster with one node, all named {name}")


def unless_gone(call, *codes):
    """Runs a delete call; returns False when the resource did not exist."""
    try:
        call()
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in codes:
            return False
        raise


def wait_gone(waiter, **kwargs):
    try:
        waiter.wait(WaiterConfig=SLOW, **kwargs)
    except WaiterError as e:
        # A waiter for "deleted" fails when the resource was never there
        if "NotFound" not in str(e) and "ResourceNotFoundException" not in str(e):
            raise


def settle(waiter, **kwargs):
    """Waits for a resource to finish being created; says nothing about whether it exists."""
    try:
        waiter.wait(WaiterConfig=SLOW, **kwargs)
    except (WaiterError, ClientError):
        pass


def delete_role(iam, role_name):
    try:
        attached = iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            return
        raise
    for policy in attached:
        iam.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])
    iam.delete_role(RoleName=role_name)


def delete(region, name):
    ec2 = client("ec2", region)
    rds = client("rds", region)
    elasticache = client("elasticache", region)
    eks = client("eks", region)
    iam = client("iam", region)

    # Something still being created cannot be deleted: let each settle first (a waiter for
    # "available" fails at once when the resource is absent or already being deleted)
    settle(rds.get_waiter("db_instance_available"), DBInstanceIdentifier=name)
    settle(rds.get_waiter("db_cluster_available"), DBClusterIdentifier=name + "-aurora")
    settle(elasticache.get_waiter("cache_cluster_available"), CacheClusterId=name)

    # Start every slow deletion, then wait for all of them
    unless_gone(lambda: rds.delete_db_instance(
        DBInstanceIdentifier=name, SkipFinalSnapshot=True, DeleteAutomatedBackups=True),
        "DBInstanceNotFound", "InvalidDBInstanceState")
    unless_gone(lambda: rds.delete_db_cluster(
        DBClusterIdentifier=name + "-aurora", SkipFinalSnapshot=True),
        "DBClusterNotFoundFault", "InvalidDBClusterStateFault")
    unless_gone(lambda: elasticache.delete_cache_cluster(CacheClusterId=name),
                "CacheClusterNotFound", "InvalidCacheClusterState")

    try:
        nodegroups = eks.list_nodegroups(clusterName=name)["nodegroups"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        nodegroups = []
    for nodegroup in nodegroups:
        unless_gone(lambda: eks.delete_nodegroup(clusterName=name, nodegroupName=nodegroup),
                    "ResourceNotFoundException", "ResourceInUseException")
    for nodegroup in nodegroups:
        wait_gone(eks.get_waiter("nodegroup_deleted"), clusterName=name, nodegroupName=nodegroup)
    settle(eks.get_waiter("cluster_active"), name=name)
    unless_gone(lambda: eks.delete_cluster(name=name), "ResourceNotFoundException")
    wait_gone(eks.get_waiter("cluster_deleted"), name=name)
    delete_role(iam, name + "-eks-cluster")
    delete_role(iam, name + "-eks-node")

    wait_gone(rds.get_waiter("db_instance_deleted"), DBInstanceIdentifier=name)
    wait_gone(rds.get_waiter("db_cluster_deleted"), DBClusterIdentifier=name + "-aurora")
    wait_gone(elasticache.get_waiter("cache_cluster_deleted"), CacheClusterId=name)

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
    unless_gone(lambda: client("ecr", region).delete_repository(repositoryName=name, force=True),
                "RepositoryNotFoundException")
    unless_gone(lambda: client("dynamodb", region).delete_table(TableName=name),
                "ResourceNotFoundException")

    left = {
        "security groups": len(ec2.describe_security_groups(Filters=tag_filter)["SecurityGroups"]),
        "eks clusters": sum(1 for c in eks.list_clusters()["clusters"] if c == name),
        "rds instances": sum(1 for d in rds.describe_db_instances()["DBInstances"]
                             if d["DBInstanceIdentifier"] == name),
        "rds clusters": sum(1 for d in rds.describe_db_clusters()["DBClusters"]
                            if d["DBClusterIdentifier"] == name + "-aurora"),
        "cache clusters": sum(1 for c in elasticache.describe_cache_clusters()["CacheClusters"]
                              if c["CacheClusterId"] == name),
        "iam roles": sum(1 for r in iam.get_paginator("list_roles").paginate()
                         for x in r["Roles"] if x["RoleName"].startswith(name + "-eks-")),
    }
    print(f"aws: terminated {len(instances)} instance(s); left: {left}")
    if any(left.values()):
        sys.exit(1)


if __name__ == "__main__":
    {"create": create, "delete": delete}[sys.argv[1]](sys.argv[2], sys.argv[3])
