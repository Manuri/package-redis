/*
 * Copyright (c) 2018, WSO2 Inc. (http:www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http:www.apache.orglicensesLICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.data.redis.actions;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.data.redis.Constants;
import org.ballerinalang.data.redis.RedisDataSource;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.exceptions.ArgumentOutOfRangeException;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.HashMap;
import java.util.Map;

/**
 * {@code {@link AbstractRedisAction}} is the base class for all Redis connector actions.
 *
 * @since 0.5.0
 */
public abstract class AbstractRedisAction extends AbstractNativeAction {

    @Override
    public BValue getRefArgument(Context context, int index) {
        if (index > -1) {
            return context.getControlStack().getCurrentFrame().getRefRegs()[index];
        }
        throw new ArgumentOutOfRangeException(index);
    }

    protected ConnectorFuture getConnectorFuture() {
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

    protected RedisDataSource getDataSource(BConnector bConnector) {
        RedisDataSource datasource = null;
        BMap sharedMap = (BMap) bConnector.getRefField(1);
        if (sharedMap.get(new BString(Constants.DATASOURCE_KEY)) != null) {
            BValue value = sharedMap.get(new BString(Constants.DATASOURCE_KEY));
            if (value instanceof RedisDataSource) {
                datasource = (RedisDataSource) value;
            }
        } else {
            throw new BallerinaException("Datasource is not initialized properly");
        }
        return datasource;
    }

    RedisCodec retrieveRedisCodec(String codecString) {
        Codec codec = retrieveCodec(codecString);
        switch (codec) {
        case BYTE_ARRAY_CODEC:
            return new ByteArrayCodec();
        case STRING_CODEC:
            return new StringCodec();
        case UTF8_STRING_CODEC:
            return new Utf8StringCodec();
        default:
            throw new UnsupportedOperationException("Support for RedisCodec " + codec + " is not implemented yet");
        }
    }

    Codec retrieveCodec(String codecString) {
        try {
            return Codec.fromCodecName(codecString);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Unsupported Codec: " + codecString);
        }
    }

    enum Codec {
        BYTE_ARRAY_CODEC("ByteArrayCodec"), STRING_CODEC("StringCodec"), UTF8_STRING_CODEC("Utf8StringCodec");

        String codec;

        static Map<String, Codec> codecMap = new HashMap<>(3);

        static {
            Codec[] codecs = values();
            for (Codec codec : codecs) {
                codecMap.put(codec.getCodecName(), codec);
            }
        }

        Codec(String codec) {
            this.codec = codec;
        }

        String getCodecName() {
            return codec;
        }

        public static Codec fromCodecName(String codecName) {
            Codec codec = codecMap.get(codecName);
            if (codec == null) {
                throw new IllegalArgumentException("Unsupported Codec: " + codecName);
            }
            return codec;
        }
    }

    protected <K, V> BString set(K key, V value, RedisDataSource<K, V> redisDataSource) {
        String result = isClusterConnection(redisDataSource) ?
                redisDataSource.getRedisClusterCommands().set(key, value) :
                redisDataSource.getRedisCommands().set(key, value);
        return new BString(result);
    }

    protected <K> BString get(K key, RedisDataSource<K, String> redisDataSource) {
        String result = isClusterConnection(redisDataSource) ?
                redisDataSource.getRedisClusterCommands().get(key) :
                redisDataSource.getRedisCommands().get(key);
        return new BString(result);
    }

    protected BInteger append(String key, String value, RedisDataSource<String, String> redisDataSource) {
        Long result = isClusterConnection(redisDataSource) ?
                redisDataSource.getRedisClusterCommands().append(key, value) :
                redisDataSource.getRedisCommands().append(key, value);
        return new BInteger(result);
    }

    protected BInteger bitcount(String key, RedisDataSource<String, String> redisDataSource) {
        Long result = isClusterConnection(redisDataSource) ?
                redisDataSource.getRedisClusterCommands().bitcount(key) :
                redisDataSource.getRedisCommands().bitcount(key);
        return new BInteger(result);
    }

    protected <K, V> BInteger lPush(K key, RedisDataSource<K, V> redisDataSource, V... value) {
        Long result = isClusterConnection(redisDataSource) ?
                redisDataSource.getRedisClusterCommands().lpush(key, value) :
                redisDataSource.getRedisCommands().lpush(key, value);
        return new BInteger(result);
    }

    protected <K> BString lPop(K key, RedisDataSource<K, String> redisDataSource) {
        String result = isClusterConnection(redisDataSource) ?
                redisDataSource.getRedisClusterCommands().lpop(key) :
                redisDataSource.getRedisCommands().lpop(key);
        return new BString(result);
    }

    protected <K, V> BInteger lpushx(K key, RedisDataSource<K, V> redisDataSource, V... value) {
        Long result = isClusterConnection(redisDataSource) ?
                redisDataSource.getRedisClusterCommands().lpush(key, value) :
                redisDataSource.getRedisCommands().lpushx(key, value);
        return new BInteger(result);
    }

    protected <K, V> BString quit(RedisDataSource<K, V> redisDataSource) {
        String result = isClusterConnection(redisDataSource) ?
                redisDataSource.getRedisClusterCommands().quit() :
                redisDataSource.getRedisCommands().quit();
        return new BString(result);
    }

    protected <K, V> BInteger bitopAnd(K destination, RedisDataSource<K, V> redisDataSource, K... keys) {
        Long result = isClusterConnection(redisDataSource) ?
                redisDataSource.getRedisClusterCommands().bitopAnd(destination, keys) :
                redisDataSource.getRedisCommands().bitopAnd(destination, keys);
        return new BInteger(result);
    }

    private boolean isClusterConnection(RedisDataSource redisDataSource) {
        return redisDataSource.isClusterConnection();
    }

}
