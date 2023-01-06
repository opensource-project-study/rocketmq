/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * 16777216 = 2 ^ 64
     */
    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    /**
     * {@link LengthFieldBasedFrameDecoder} 将帧的长度编码到帧的头部来定义帧，参考《Netty实战》“11.4.2　基于长度的协议”<p/>
     * {@link LengthFieldBasedFrameDecoder} 构造方法中有很多参数，参数的含义和示例可以查看netty项目源码中LengthFieldBasedFrameDecoder类上的Javadoc注释，注释非常详细<p/>
     * 简要摘录{@link LengthFieldBasedFrameDecoder}构造方法常用的参数含义：
     *
     * <pre>
     * param maxFrameLength
     *        最大帧长度
     *        the maximum length of the frame.  If the length of the frame is
     *        greater than this value, {@link io.netty.handler.codec.TooLongFrameException} will be
     *        thrown.
     * param lengthFieldOffset
     *        长度字段在帧中的偏移量
     *        the offset of the length field
     * param lengthFieldLength
     *        长度字段占用的字节数
     *        the length of the length field
     * param lengthAdjustment
     *        "长度字段"表示的值的补偿值，用于校正"长度字段"表示的值，确保（"长度字段"表示的值 + 补偿值 == 帧中"长度字段"之后的字节总数）
     *        the compensation value to add to the value of the length field
     * param initialBytesToStrip
     *        帧中前多少个字节要被去除掉
     *        the number of first bytes to strip out from the decoded frame
     * </pre>
     *
     */
    public NettyDecoder() {
        // 帧中从第0个字节起数4个字节表示"长度字段"，该"长度字段"的值表示该帧中"长度字段"之后的字节总数
        // 第3个参数为0，表示"长度字段"的值的补偿值为0，即不对"长度字段"的值做校正
        // 第4个参数为4表示去除该帧中前4个字节
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }

            // 注意，这里的ByteBuffer中的字节流已经去除了前4个字节，即帧中的"长度字段"已经被去除
            ByteBuffer byteBuffer = frame.nioBuffer();

            return RemotingCommand.decode(byteBuffer);
        } catch (Exception e) {
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            if (null != frame) {
                frame.release();
            }
        }

        return null;
    }
}
