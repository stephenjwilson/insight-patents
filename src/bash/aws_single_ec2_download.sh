#!/usr/bin/env bash
source .env
export REPO="insight-patents"

# Make ec2 instance

INSTANCE_ID=$(aws ec2 run-instances --image-id ami-04169656fea786776 --count 1 --instance-type t2.micro --key-name $KEYPAIR --security-group-ids $SECURITY_GROUP --subnet-id $SUBNET --query 'Instances[0].InstanceId')

# Run download script TODO: Figure out why instance id doesn't work
#aws ssm send-command --document-name "AWS-RunRemoteScript" --instance-ids $INSTANCE_ID --parameters '{"sourceType":["GitHub"],"sourceInfo":["{\"owner\":\"stephenjwilson\", \"repository\": \"$REPO\", \"path\": \"src/bash/download_patents.sh\" }"],"commandLine":["bash download_patents.sh"]}'

#aws ec2 terminate-instances --instance-ids $INSTANCE_ID