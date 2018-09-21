# Copyright (c) 2016 Six After, Inc. All Rights Reserved. 
#
# Use of this source code is governed by the Apache 2.0 license that can be
# found in the LICENSE file in the root of the source
# tree.

output "master_id" {
  value = "${aws_instance.master_node.id}"
}

output "master_public_dns" {
  value = "${aws_instance.master_node.public_dns}"
}

output "master_public_ip" {
  value = "${aws_instance.master_node.public_ip}"
}

output "master_private_dns" {
  value = "${aws_instance.master_node.private_dns}"
}

output "master_private_ip" {
  value = "${aws_instance.master_node.private_ip}"
}

output "slave_id" {
  value = "${join(",",aws_instance.slave_node.*.id)}"
}

output "slave_public_dns" {
  value = "${join(",",aws_instance.slave_node.*.public_dns)}"
}

output "slave_public_ip" {
  value = "${join(",",aws_instance.slave_node.*.public_ip)}"
}

output "slave_private_dns" {
  value = "${join(",",aws_instance.slave_node.*.private_dns)}"
}

output "slave_private_ip" {
  value = "${join(",",aws_instance.slave_node.*.private_ip)}"
}

