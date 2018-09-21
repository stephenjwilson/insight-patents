#!/usr/bin/env bash
#
# Copyright (c) 2016 Six After, Inc. All Rights Reserved. 
#
# Use of this source code is governed by the Apache 2.0 license that can be
# found in the LICENSE file in the root of the source
# tree.

echo "Generating cluster's SSH key on master..."
if [ -f ~/.ssh/id_rsa ] ||
    (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
     cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys); then

  echo "Cluster SSH keys generated successfully."
fi

