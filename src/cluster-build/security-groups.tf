# Copyright (c) 2016 Six After, Inc. All Rights Reserved. 
#
# Use of this source code is governed by the Apache 2.0 license that can be
# found in the LICENSE file in the root of the source
# tree.

# Our default security group for the master nodes.
resource "aws_security_group" "default_master" {
  depends_on = [
    "aws_vpc.default"]

  name = "${var.cluster_name}-master"
  description = "Spark Security Group: Master"
  vpc_id = "${aws_vpc.default.id}"

  # SSH access from anywhere.
  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = [
      "104.247.55.102/32"]
  }

  # Outbound internet access.
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [
      "0.0.0.0/0"]
  }

  //  # HDFS NFS gateway requires 111, 2049, 4242 for TCP and UDP.
  //  ingress {
  //    from_port = 111
  //    to_port = 111
  //    protocol = "tcp"
  //    cidr_blocks = ["0.0.0.0/0"]
  //  }
  //
  //  ingress {
  //    from_port = 111
  //    to_port = 111
  //    protocol = "udp"
  //    cidr_blocks = ["0.0.0.0/0"]
  //  }
  //
  //  ingress {
  //    from_port = 2049
  //    to_port = 2049
  //    protocol = "tcp"
  //    cidr_blocks = ["0.0.0.0/0"]
  //  }
  //
  //  ingress {
  //    from_port = 2049
  //    to_port = 2049
  //    protocol = "udp"
  //    cidr_blocks = ["0.0.0.0/0"]
  //  }
  //
  //  ingress {
  //    from_port = 4242
  //    to_port = 4242
  //    protocol = "tcp"
  //    cidr_blocks = ["0.0.0.0/0"]
  //  }
  //
  //  ingress {
  //    from_port = 4242
  //    to_port = 4242
  //    protocol = "udp"
  //    cidr_blocks = ["0.0.0.0/0"]
  //  }
  //
  //  # RM in YARN mode uses 8088.
  //  ingress {
  //    from_port = 8088
  //    to_port = 8088
  //    protocol = "tcp"
  //    cidr_blocks = ["0.0.0.0/0"]
  //  }
  //
  //  # Ganglia.
  //  ingress {
  //    from_port = 5080
  //    to_port = 5080
  //    protocol = "tcp"
  //    self = true
  //  }

  # Inbound TCP and UDP allowed for any server in this security group.
  ingress {
    from_port = 1
    to_port = 65535
    protocol = "tcp"
    self = true
  }

  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    self = true
  }

  ingress {
    from_port = 1
    to_port = 65535
    protocol = "udp"
    self = true
  }

  # We sleep for a few seconds to allow for propagation of AWS metadata to complete.
  provisioner "local-exec" {
    command = "sleep 20"
  }

  tags {
    Name = "Spark.Master"
  }
}

# Our default security group for the slave nodes.
resource "aws_security_group" "default_slave" {
  depends_on = [
    "aws_vpc.default",
    "aws_security_group.default_master"]

  name = "${var.cluster_name}-slave"
  description = "Spark Security Group: Slaves"
  vpc_id = "${aws_vpc.default.id}"

  # SSH access from anywhere.
  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = [
      "104.247.55.102/32"]
  }

  # Outbound internet access.
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [
      "0.0.0.0/0"]
  }

  //  # Ganglia.
  //  ingress {
  //    from_port = 5080
  //    to_port = 5080
  //    protocol = "tcp"
  //    self = true
  //  }

  # Inbound TCP and UDP allowed for any server in this security group.
  ingress {
    from_port = 1
    to_port = 65535
    protocol = "tcp"
    self = true
  }

  ingress {
    from_port = 1
    to_port = 65535
    protocol = "udp"
    self = true
  }

  # Inbound TCP and UDP allowed for any server in the master security group.
  ingress {
    from_port = 1
    to_port = 65535
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.default_master.id}"]
  }

  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.default_master.id}"]
  }


  ingress {
    from_port = 1
    to_port = 65535
    protocol = "udp"
    security_groups = [
      "${aws_security_group.default_master.id}"]
  }

  # We sleep for a few seconds to allow for propagation of AWS metadata to complete.
  provisioner "local-exec" {
    command = "sleep 20"
  }

  tags {
    Name = "Spark.Slave"
  }
}

# Inbound TCP allowed from any server in the slave security group to the master.
resource "aws_security_group_rule" "allow_all_TCP_slave_to_master" {
  depends_on = [
    "aws_security_group.default_slave",
    "aws_security_group.default_master"]
  type = "ingress"
  from_port = 1
  to_port = 65535
  protocol = "tcp"
  security_group_id = "${aws_security_group.default_master.id}"
  source_security_group_id = "${aws_security_group.default_slave.id}"
}

# Inbound UDP allowed from any server in the slave security group to the master.
resource "aws_security_group_rule" "allow_all_UDP_slave_to_master" {
  depends_on = [
    "aws_security_group.default_slave",
    "aws_security_group.default_master"]
  type = "ingress"
  from_port = 1
  to_port = 65535
  protocol = "udp"
  security_group_id = "${aws_security_group.default_master.id}"
  source_security_group_id = "${aws_security_group.default_slave.id}"
}
