# Copyright (c) 2016 Six After, Inc. All Rights Reserved. 
#
# Use of this source code is governed by the Apache 2.0 license that can be
# found in the LICENSE file in the root of the source
# tree.

# Specify the provider and access details.
provider "aws" {
  access_key = "${var.aws_access_key}"
  secret_key = "${var.aws_secret_key}"
  region = "${var.aws_region}"
}

##
## Virtual Private Cloud (VPC).
##
resource "aws_vpc" "default" {
  cidr_block = "${var.vpc_cidr_block}"
  enable_dns_hostnames = true
  enable_dns_support = true

  tags {
    Name = "${var.cluster_name}-vpc"
  }
}

##
## Internet Gateway.
##
resource "aws_internet_gateway" "default" {
  depends_on = [
    "aws_vpc.default"]
  vpc_id = "${aws_vpc.default.id}"

  tags {
    Name = "${var.cluster_name}-gateway"
  }
}
