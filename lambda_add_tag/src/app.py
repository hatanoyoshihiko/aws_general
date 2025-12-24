import os
import boto3

ENV_TAG_KEY = os.getenv("TAG_KEY", "env")
ENV_TAG_VALUE = os.getenv("TAG_VALUE", "dev")

ec2 = boto3.client("ec2")
rds = boto3.client("rds")


def has_tag_key(tags, key: str) -> bool:
    return any(t.get("Key") == key for t in (tags or []))


# ----------------------
# EC2
# ----------------------
def tag_ec2_instances():
    tagged = 0
    skipped = 0

    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate():
        for r in page.get("Reservations", []):
            for i in r.get("Instances", []):
                if has_tag_key(i.get("Tags"), ENV_TAG_KEY):
                    skipped += 1
                    continue

                ec2.create_tags(
                    Resources=[i["InstanceId"]],
                    Tags=[{"Key": ENV_TAG_KEY, "Value": ENV_TAG_VALUE}],
                )
                tagged += 1

    return {"tagged": tagged, "skipped": skipped}


# ----------------------
# RDS / Aurora Instance
# ----------------------
def tag_rds_instances():
    tagged = 0
    skipped = 0

    paginator = rds.get_paginator("describe_db_instances")
    for page in paginator.paginate():
        for db in page.get("DBInstances", []):
            arn = db["DBInstanceArn"]

            tags = rds.list_tags_for_resource(
                ResourceName=arn
            ).get("TagList", [])

            if has_tag_key(tags, ENV_TAG_KEY):
                skipped += 1
                continue

            rds.add_tags_to_resource(
                ResourceName=arn,
                Tags=[{"Key": ENV_TAG_KEY, "Value": ENV_TAG_VALUE}],
            )
            tagged += 1

    return {"tagged": tagged, "skipped": skipped}


# ----------------------
# Aurora Cluster
# ----------------------
def tag_aurora_clusters():
    tagged = 0
    skipped = 0

    paginator = rds.get_paginator("describe_db_clusters")
    for page in paginator.paginate():
        for cluster in page.get("DBClusters", []):
            arn = cluster["DBClusterArn"]

            tags = rds.list_tags_for_resource(
                ResourceName=arn
            ).get("TagList", [])

            if has_tag_key(tags, ENV_TAG_KEY):
                skipped += 1
                continue

            rds.add_tags_to_resource(
                ResourceName=arn,
                Tags=[{"Key": ENV_TAG_KEY, "Value": ENV_TAG_VALUE}],
            )
            tagged += 1

    return {"tagged": tagged, "skipped": skipped}


def lambda_handler(event, context):
    return {
        "tag_key": ENV_TAG_KEY,
        "tag_value": ENV_TAG_VALUE,
        "ec2": tag_ec2_instances(),
        "rds_instances": tag_rds_instances(),
        "aurora_clusters": tag_aurora_clusters(),
    }
