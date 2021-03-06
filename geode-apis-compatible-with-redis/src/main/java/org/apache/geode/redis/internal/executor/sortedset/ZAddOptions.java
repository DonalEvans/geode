/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.geode.redis.internal.executor.sortedset;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.executor.BaseSetOptions;


/**
 * Class representing different options that can be used with Redis Sorted Set ZADD command.
 */
public class ZAddOptions extends BaseSetOptions {
  private boolean isCH;
  private boolean isINCR;

  public ZAddOptions(Exists existsOption, boolean isCH, boolean isINCR) {
    super(existsOption);

    this.isCH = isCH;
    this.isINCR = isINCR;
  }

  public boolean isCH() {
    return isCH;
  }

  public boolean isINCR() {
    return isINCR;
  }

  public ZAddOptions() {}

  @Override
  public int getDSFID() {
    return REDIS_SORTED_SET_OPTIONS_ID;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeBoolean(isCH);
    out.writeBoolean(isINCR);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context) throws IOException {
    super.fromData(in, context);
    isCH = in.readBoolean();
    isINCR = in.readBoolean();
  }

}
