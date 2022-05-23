#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Prerequisites
# 1. update change log
# 2. clear milestone issues, and create a new one if needed
# 3. export VERSION=<the version to release>

set -ex

RELEASE_VERSION=${RELEASE_VERSION}
TAG_NAME=v${RELEASE_VERSION}
PRODUCT_NAME="apache-skywalking-banyandb-java-client"

echo "Release version "${RELEASE_VERSION}
echo "Source tag "${TAG_NAME}

if [ "$RELEASE_VERSION" == "" ]; then
  echo "RELEASE_VERSION environment variable not found, Please setting the RELEASE_VERSION."
  echo "For example: export RELEASE_VERSION=5.0.0-alpha"
  exit 1
fi

echo "Creating source package"

PRODUCT_NAME=${PRODUCT_NAME}-${RELEASE_VERSION}

rm -rf ${PRODUCT_NAME}
mkdir ${PRODUCT_NAME}

git clone https://github.com/apache/skywalking-banyandb-java-client ./${PRODUCT_NAME}
cd ${PRODUCT_NAME}

TAG_EXIST=`git tag -l ${TAG_NAME} | wc -l`

if [ ${TAG_EXIST} -ne 1 ]; then
    echo "Could not find the tag named" ${TAG_NAME}
    exit 1
fi

git checkout ${TAG_NAME}

cd ..

rm -rf skywalking

svn co https://dist.apache.org/repos/dist/dev/skywalking/
mkdir -p skywalking/banyandb-java-client/"$RELEASE_VERSION"
cp "${PRODUCT_NAME}-src.tgz" skywalking/banyandb-java-client/"$RELEASE_VERSION"
cp "${PRODUCT_NAME}-src.tgz.asc" skywalking/banyandb-java-client/"$RELEASE_VERSION"
cp "${PRODUCT_NAME}-src.tgz.sha512" skywalking/banyandb-java-client/"$RELEASE_VERSION"

cd skywalking/banyandb-java-client && svn add "$RELEASE_VERSION" && svn commit -m "Draft Apache SkyWalking-BanyanDB Java Client release $RELEASE_VERSION"

cd ../..

cat << EOF
=========================================================================
Subject: [VOTE] Release Apache SkyWalking BanyanDB Java Client - $RELEASE_VERSION
Content:
Hi The SkyWalking Community:
This is a call for vote to release Apache SkyWalking BanyanDB Java Client - $RELEASE_VERSION.
Release Candidate:
 * https://dist.apache.org/repos/dist/dev/skywalking/banyandb-java-client/$RELEASE_VERSION/
 * sha512 checksums
   - $(cat ${PRODUCT_NAME}-src.tgz.sha512)
Release Tag :
 * (Git Tag) $TAG_NAME
Release CommitID :
 * https://github.com/apache/skywalking-banyandb-java-client/tree/$(git rev-list -n 1 "$TAG_NAME")
Keys to verify the Release Candidate :
 * https://dist.apache.org/repos/dist/release/skywalking/KEYS
Guide to build the release from source :
 * https://github.com/apache/skywalking-banyandb-java-client/blob/$TAG_NAME/README.md#compiling-project
Voting will start now and will remain open for at least 72 hours, all PMC members are required to give their votes.
[ ] +1 Release this package.
[ ] +0 No opinion.
[ ] -1 Do not release this package because....
Thanks.
EOF