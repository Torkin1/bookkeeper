#!/usr/bin/env sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# $1: ba-dua release

WORKDIR=$(mktemp -d)
BADUA_RELEASE="ba-dua-$1"
BADUA_FROM="https://github.com/saeg/ba-dua/archive/refs/tags/$BADUA_RELEASE.zip"
toUnzip="$BADUA_RELEASE.zip"

cd "$WORKDIR" || exit 
wget "$BADUA_FROM" -O "$toUnzip"
unzip "$toUnzip"
cd "ba-dua-$BADUA_RELEASE" || exit
mvn clean install
