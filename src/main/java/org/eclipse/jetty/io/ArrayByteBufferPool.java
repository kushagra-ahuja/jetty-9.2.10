//
//  ========================================================================
//  Copyright (c) 1995-2015 Mort Bay Consulting Pty. Ltd.
//  ------------------------------------------------------------------------
//  All rights reserved. This program and the accompanying materials
//  are made available under the terms of the Eclipse Public License v1.0
//  and Apache License v2.0 which accompanies this distribution.
//
//      The Eclipse Public License is available at
//      http://www.eclipse.org/legal/epl-v10.html
//
//      The Apache License v2.0 is available at
//      http://www.opensource.org/licenses/apache2.0.php
//
//  You may elect to redistribute this code under either of these licenses.
//  ========================================================================
//

package org.eclipse.jetty.io;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.eclipse.jetty.util.BufferUtil;

public class ArrayByteBufferPool implements ByteBufferPool
{
    private final int _min;
    private final Bucket[] _direct;
    private final Bucket[] _indirect;
    private final int _inc;

    public ArrayByteBufferPool()
    {
        this(0,1024,64*1024);
    }

    public ArrayByteBufferPool(int minSize, int increment, int maxSize)
    {
        if (minSize>=increment)
            throw new IllegalArgumentException("minSize >= increment");
        if ((maxSize%increment)!=0 || increment>=maxSize)
            throw new IllegalArgumentException("increment must be a divisor of maxSize");
        _min=minSize;
        _inc=increment;

        _direct=new Bucket[maxSize/increment];
        _indirect=new Bucket[maxSize/increment];

        int size=0;
        for (int i=0;i<_direct.length;i++)
        {
            size+=_inc;
            _direct[i]=new Bucket(size);
            _indirect[i]=new Bucket(size);
        }
    }

    @Override
    public ByteBuffer acquire(int size, boolean direct){
    	//bucketFor根据容量大小size和direct定位到对应的桶Bucket
        Bucket bucket = bucketFor(size,direct);
        //如果Bucket存在则查看其队列_queue种是否有可供使用的ByteBuffer
        ByteBuffer buffer = bucket==null?null:bucket._queue.poll();

        //如果Bucket队列_queue中没有可供使用的ByteBuffer，就用BufferUtil分配新的ByteBuffer
        if (buffer == null){
            int capacity = bucket==null?size:bucket._size;
            buffer = direct ? BufferUtil.allocateDirect(capacity) 
            		        : BufferUtil.allocate(capacity);
        }
        return buffer;
    }

    @Override
    public void release(ByteBuffer buffer){
        if (buffer!=null){    
        	//bucketFor根据ByteBuffer容量大小capacity和direct定位到对应的桶Bucket
            Bucket bucket = bucketFor(buffer.capacity(),buffer.isDirect());
            //如果Bucket存在则使用BufferUtil清理ByteBuffer，然后放回Bucket的队列_queue里面以便以后复用
            if (bucket!=null){
                BufferUtil.clear(buffer);
                bucket._queue.offer(buffer);
            }
        }
    }

    //根据给定的容量大小size和direct的值去定位对应的Bucket
    private Bucket bucketFor(int size,boolean direct){
        if (size<=_min) return null;
        int b=(size-1)/_inc; //定位Bucket索引位置
        if (b>=_direct.length) return null;
        Bucket bucket = direct?_direct[b]:_indirect[b];
        return bucket;
    }

    public static class Bucket
    {
        public final int _size;
        public final Queue<ByteBuffer> _queue= new ConcurrentLinkedQueue<>();

        Bucket(int size)
        {
            _size=size;
        }
        
        @Override
        public String toString()
        {
            return String.format("Bucket@%x{%d,%d}",hashCode(),_size,_queue.size());
        }
    }
    
    public void clear()
    {
        for (int i=0;i<_direct.length;i++)
        {
            _direct[i]._queue.clear();
            _indirect[i]._queue.clear();
        }
    }
    

    // Package local for testing
    Bucket[] bucketsFor(boolean direct)
    {
        return direct ? _direct : _indirect;
    }
}
