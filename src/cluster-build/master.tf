# Copyright (c) 2016 Six After, Inc. All Rights Reserved. 
#
# Use of this source code is governed by the Apache 2.0 license that can be
# found in the LICENSE file in the root of the source
# tree.

resource "aws_instance" "master_node" {
  depends_on = [
    "aws_instance.slave_node"]

  # The connection block tells our provisioner how to
  # communicate with the resource (instance).
  connection {
    # The default username for our AMI
    user = "ubuntu"

    # The path to your keyfile
    //    private_key = "${file("/Users/wilson/.ssh/swilson-IAM-keypair.pem")}"
    private_key = "${file("~/.ssh/swilson-IAM-keypair.pem")}"

    type = "ssh"
    host = "${self.public_ip}"
  }

  instance_type = "${var.aws_ec2_instance_type}"

  # Lookup the correct AMI based on the region
  # we specified.
  ami = "${lookup(var.aws_amis, var.aws_region)}"

  # The name of our SSH key-pair.
  key_name = "${var.ssh_key_name}"

  # Our security group to allow SSH access.
  security_groups = [
    "${aws_security_group.default_master.id}"]
  subnet_id = "${aws_subnet.public.id}"
  vpc_security_group_ids = [
    "${aws_security_group.default_master.id}"]
  associate_public_ip_address = true

  ebs_optimized = "${lookup(var.ebs_optimized, var.aws_ec2_instance_type)}"

  root_block_device {
    volume_type = "gp2"
    volume_size = 2000
  }

  # Allow the AWS insfrastructure metadata to propagate.
  provisioner "local-exec" {
    command = "sleep 15"
  }

  # This copies the dynamically generated list of slave node TCP/IP addresses
  # required by Spark.
  provisioner "file" {
    source = "${path.module}/output/slaves"
    destination = "/tmp/slaves"
  }

  # Stage and execute the script common to master and slave nodes. I have no common setup
  //  provisioner "file" {
  //    source = "${path.module}/sbin/setup-common.sh"
  //    destination = "/tmp/setup-common.sh"
  //  }

  provisioner "remote-exec" {
    inline = [
      "sudo echo 'export AWS_ACCESS_KEY_ID=${var.aws_access_key}' >> $HOME/.profile",
      "sudo echo 'export AWS_SECRET_ACCESS_KEY=${var.aws_secret_key}' >> $HOME/.profile",
    ]
  }
  #       "sudo bash /tmp/setup-common.sh"

  # Stage and execute the master node configuration script.
  provisioner "file" {
    source = "${path.module}/sbin/setup-master.sh"
    destination = "/tmp/setup-master.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "bash /tmp/setup-master.sh"
    ]
  }

  # Copy the master node's newly generated SSH public and private keys to the local machine 
  # so we can deploy both to all slaves.
  provisioner "local-exec" {
    # The /dev/null file is a special system device file that discards anything and everything
    # written to it, and when used as the input file, returns End Of File immediately. By 
    # configuring the null device file as the host key database, SSH is fooled into thinking
    # the SSH client has never connected to any SSH server before, and so will never run into 
    # a mismatched host key.
    command = "scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ${var.ssh_key_path} ubuntu@${self.public_ip}:/home/ubuntu/.ssh/id_rsa.pub ${path.module}/output/"
  }

  provisioner "local-exec" {
    command = "scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ${var.ssh_key_path} ubuntu@${self.public_ip}:/home/ubuntu/.ssh/id_rsa ${path.module}/output/"
  }

  tags {
    Name = "${var.cluster_name}.master"
  }
}

# Note: The "null_resource" is an undocumented feature until the no-op logical provider feature
# added. See the following references for more infomration.
# : https://github.com/hashicorp/terraform/issues/580
# : https://github.com/hashicorp/terraform/issues/1178
resource "null_resource" "slave_ssh_provisioner" {
  depends_on = [
    "aws_instance.master_node"]

  count = "${var.cluster_size}"
  connection {
    user = "ubuntu"
    private_key = "${file("~/.ssh/swilson-IAM-keypair.pem")}"
    type = "ssh"
    timeout = "1m"
    host = "${element(aws_instance.slave_node.*.public_ip, count.index)}"
    #host = "${aws_instance.slave_node.*.public_ip}"
  }

  # Send the SSH public key up to the slave and add it to the authorized_hosts file.
  provisioner "file" {
    source = "${path.module}/output/id_rsa.pub"
    destination = "~/.ssh/id_rsa.pub"
  }

  # Send the SSH private key up to the slave and add it to the authorized_hosts file.
  provisioner "file" {
    source = "${path.module}/output/id_rsa"
    destination = "~/.ssh/id_rsa"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys"
    ]
  }
}

