# Copyright (c) 2016 Six After, Inc. All Rights Reserved. 
#
# Use of this source code is governed by the Apache 2.0 license that can be
# found in the LICENSE file in the root of the source
# tree.

resource "aws_instance" "slave_node" {
  depends_on = [
    "aws_security_group.default_slave"]

  # The connection block tells our provisioner how to communicate with the 
  # resource (instance).
  connection {
    # The default username for our AMI
    user = "ubuntu"

    # The path to your keyfile
    private_key = "${file("/Users/wilson/.ssh/swilson-IAM-keypair.pem")}"

    type = "ssh"
    host = "${self.public_ip}"
    timeout = "1m"
  }

  instance_type = "${var.aws_ec2_instance_type}"

  # Lookup the correct AMI based on the region we specified.
  ami = "${lookup(var.aws_amis, var.aws_region)}"

  ebs_optimized = "${lookup(var.ebs_optimized, var.aws_ec2_instance_type)}"

  # The name of our SSH key-pair.
  key_name = "${var.ssh_key_name}"

  # Our Security group to allow SSH access.
  security_groups = [
    "${aws_security_group.default_slave.id}"]
  vpc_security_group_ids = [
    "${aws_security_group.default_slave.id}"]
  subnet_id = "${aws_subnet.public.id}"
  associate_public_ip_address = true

  root_block_device {
    volume_type = "gp2"
    volume_size = 1000
  }

  count = "${var.cluster_size}"

  #user_data = "${file("setup_os.sh")}"

  provisioner "local-exec" {
    command = "echo ${self.private_ip} >> ${path.module}/output/slaves"
  }

  # Allow AWS insfrastructure metadata to propagate.
  provisioner "local-exec" {
    command = "sleep 15"
  }

  //  # Stage the Spark deployment and configuration script.
  //  provisioner "file" {
  //    source = "${path.module}/sbin/setup-common.sh"
  //    destination = "/tmp/setup-common.sh"
  //  }

  # Execute the Spark deployment and configuration script.
  provisioner "remote-exec" {
    inline = [
      "sudo echo 'export AWS_ACCESS_KEY_ID=${var.aws_access_key}' >> $HOME/.profile",
      "sudo echo 'export AWS_SECRET_ACCESS_KEY=${var.aws_secret_key}' >> $HOME/.profile",
    ]
  }

  tags {
    Name = "${var.cluster_name}.slave.${count.index}"
  }
}
