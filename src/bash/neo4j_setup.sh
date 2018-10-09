#!/usr/bin/env bash

# Create keypair
export KEY_NAME="Neo4j-AWSMarketplace-Key"
aws ec2 create-key-pair \
  --key-name $KEY_NAME \
  --query 'KeyMaterial' \
  --output text > $KEY_NAME.pem

# Create security group
export GROUP="neo4j-sg"
aws ec2 create-security-group \
  --group-name $GROUP \
  --description "Neo4j security group"

# Open ports
for port in 22 7474 7473 7687; do
  aws ec2 authorize-security-group-ingress --group-name $GROUP --protocol tcp --port $port --cidr 0.0.0.0/0
done

# Spin up instance. Ami image for us-east-1 neo4j-community-3.0.3_on_ubuntu-16.04-xenial-xerus
aws ec2 run-instances \
  --image-id ami-3c17942b \
  --count 1 \
  --instance-type m4.large \
  --key-name $KEY_NAME \
  --security-groups $GROUP \
  --query "Instances[*].InstanceId" \
  --region us-east-1
