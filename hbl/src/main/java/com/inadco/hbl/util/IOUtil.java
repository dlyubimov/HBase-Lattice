/*
 * 
 *  Copyright © 2010, 2011 Inadco, Inc. All rights reserved.
 *  
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *  
 *  
 */
package com.inadco.hbl.util;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.springframework.context.support.AbstractApplicationContext;

/**
 * common IO utils provided in hope they would be useful
 * 
 * @author lyubimovd
 */
public final class IOUtil {

    private static final Logger s_log = Logger.getLogger(IOUtil.class);

    /**
     * make sure to close all sources, log all of the problems occurred, clear
     * <code>closeables</code> (to prevent repeating close attempts), re-throw
     * the last one at the end. Helps resource scope management (e.g.
     * compositions of {@link Closeable}s objects)
     * <P>
     * <p/>
     * Typical pattern:
     * <p/>
     * 
     * <pre>
     *   LinkedList<Closeable> closeables = new LinkedList<Closeable>();
     *   try {
     *      InputStream stream1 = new FileInputStream(...);
     *      closeables.addFirst(stream1);
     *      ...
     *      InputStream streamN = new FileInputStream(...);
     *      closeables.addFirst(streamN);
     *      ...
     *   } finally {
     *      IOUtils.closeAll(closeables);
     *   }
     * </pre>
     * 
     * @param closeables
     *            must be a modifiable collection of {@link Closeable}s
     * @throws IOException
     *             the last exception (if any) of all closed resources
     */
    public static void closeAll(Collection<? extends Closeable> closeables) throws IOException {
        Throwable lastThr = null;

        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (Throwable thr) {
                s_log.error(thr.getMessage(), thr);
                lastThr = thr;
            }
        }

        // make sure we don't double-close
        // but that has to be modifiable collection
        closeables.clear();

        if (lastThr != null) {
            if (lastThr instanceof IOException) {
                throw (IOException) lastThr;
            } else if (lastThr instanceof RuntimeException) {
                throw (RuntimeException) lastThr;
            } else if (lastThr instanceof Error) {
                throw (Error) lastThr;
            }
            // should not happen
            else {
                throw (IOException) new IOException("Unexpected exception during close").initCause(lastThr);
            }
        }

    }

    public static void closeSpringAll(Collection<? extends AbstractApplicationContext> closeables) throws IOException {

        Throwable lastThr = null;

        for (AbstractApplicationContext closeable : closeables) {
            try {
                closeable.close();
            } catch (Throwable thr) {
                s_log.error(thr.getMessage(), thr);
                lastThr = thr;
            }
        }

        // make sure we don't double-close
        // but that has to be modifiable collection
        closeables.clear();

        if (lastThr != null) {
            if (lastThr instanceof IOException) {
                throw (IOException) lastThr;
            } else if (lastThr instanceof RuntimeException) {
                throw (RuntimeException) lastThr;
            } else if (lastThr instanceof Error) {
                throw (Error) lastThr;
            }
            // should not happen
            else {
                throw (IOException) new IOException("Unexpected exception during close").initCause(lastThr);
            }
        }

    }

    /**
     * convienience wrapper
     * 
     * @param <T>
     *            the cloning type
     * @param src
     *            the object being cloned
     * @return a clone of the object, or <code>src</code> if clone is not
     *         supported.
     */
    public static <T> T tryClone(T src) {
        try {
            Method cloneMethod = src.getClass().getMethod("clone");
            @SuppressWarnings("unchecked")
            T result = (T) cloneMethod.invoke(src);
            return result;
        } catch (NoSuchMethodException exc) {
            return src;
        } catch (InvocationTargetException exc) {
            if (exc.getTargetException() instanceof CloneNotSupportedException) {
                return src;
            } else {
                throw new RuntimeException(exc.getTargetException());
            }
        } catch (IllegalAccessException exc) {
            return src;
        }
    }

    /**
     * clone writable thru its serialization, optionally initializing it with
     * the supplied configuration.
     * <P>
     * 
     * This operation is not terribly fast, so to make it faster by default,
     * client should probably maintain its own serialization buffer (or use
     * ReflectionUtils#clone if configuration is avaialble).
     * 
     * @param <T>
     * @param w
     *            writable to clone
     * @param c
     *            configuration, may be null
     */
    public static <T extends Writable> T cloneWritable(Writable w, Configuration c) {
        try {
            @SuppressWarnings("unchecked")
            T clone = (T) ReflectionUtils.newInstance(w.getClass(), c);
            if (c == null) {
                DataOutputBuffer dob = new DataOutputBuffer();
                w.write(dob);
                dob.close();
                DataInputBuffer dib = new DataInputBuffer();
                dib.reset(dob.getData(), dob.getLength());
                clone.readFields(dib);
                dib.close();
            } else
                ReflectionUtils.copy(c, w, clone);
            return clone;

        } catch (IOException exc) {
            throw new RuntimeException(exc); // should not happen as it is all
                                             // in memory...
        }
    }

    /**
     * a wrapping proxy for interfaces implementing Closeable. it implements
     * two-state fail-fast state pattern which basically has two states: before
     * close and after. any attempt to call resource method after it has been
     * closed would fail.
     * <P>
     * 
     * But it does not to make attempt to wait till the end of current
     * invocations to complete if close() is called, which means it may be
     * possible to actually attempt to invoke close() twice if attempts to close
     * made before any of them had actually completed. Which is why it is
     * fail-fast detection, i.e. no attempt to serialize invocations is made.
     * <P>
     * 
     */

    public static <T extends Closeable> T wrapCloseable(final T delegate, Class<T> iface) {
        return iface.cast(Proxy.newProxyInstance(delegate.getClass().getClassLoader(),
                                                 new Class<?>[] { iface },
                                                 new InvocationHandler() {

                                                     private boolean m_closedState = false;

                                                     @Override
                                                     public Object invoke(Object proxy, Method method, Object[] args)
                                                         throws Throwable {

                                                         if (m_closedState) {
                                                             throw new IOException(
                                                                 "attempt to invoke a closed resource.");
                                                         }
                                                         try {
                                                             if (method.equals(s_closeMethod)) {
                                                                 m_closedState = true;
                                                             } else if (method.equals(s_equalsMethod)
                                                                 && proxy == args[0]) {
                                                                 args[0] = delegate; // fix
                                                                                     // identity
                                                                                     // equality
                                                                                     // sematics
                                                             }

                                                             return method.invoke(delegate, args);
                                                         } catch (InvocationTargetException exc) {
                                                             throw exc.getTargetException();
                                                         }
                                                     }
                                                 }));
    }

    private static final Method s_closeMethod;
    private static final Method s_equalsMethod;

    static {
        try {
            s_closeMethod = Closeable.class.getMethod("close");
            s_equalsMethod = Object.class.getMethod("equals", new Class<?>[] { Object.class });
        } catch (NoSuchMethodException exc) {
            // should not happen
            throw new RuntimeException(exc);
        }
    }

    public static byte[] toByteArray(Object obj) throws java.io.IOException {
        if (obj == null)
            return null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        oos.close();
        bos.close();
        byte[] data = bos.toByteArray();
        return data;
    }

    public static class DeleteFileOnClose implements Closeable {

        private File m_file;

        public DeleteFileOnClose(File file) {
            m_file = file;
        }

        @Override
        public void close() throws IOException {
            if (m_file.isFile())
                m_file.delete();
        }
    }

    public static class DeletePathOnClose implements Closeable {
        private Path       m_path;
        private FileSystem m_fs;

        public DeletePathOnClose(Path path, FileSystem fs) {
            super();
            Validate.notNull(path);
            Validate.notNull(fs);
            this.m_path = path;
            this.m_fs = fs;
        }

        @Override
        public void close() throws IOException {
            m_fs.delete(m_path, true);
        }
    }

    public static class ExecutorServiceCloseable implements Closeable {
        private ExecutorService m_service;
        private int             m_shutdownSeconds;

        public ExecutorServiceCloseable(ExecutorService service, int secondsToWaitForShutdown) {
            super();
            m_service = service;
            m_shutdownSeconds = secondsToWaitForShutdown;
        }

        @Override
        public void close() throws IOException {
            m_service.shutdown();
            try {
                m_service.awaitTermination(m_shutdownSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException exc) {
                throw new IOException("Interrupted", exc);
            } finally {
                m_service.shutdownNow();
            }
        }
    }

    public static class FutureCloseable implements Closeable {
        private Future<?> future;
        private long      wait;
        private TimeUnit  tu;
        
        public FutureCloseable(Future<?> future, long wait, TimeUnit tu) {
            super();
            this.future = future;
            this.wait = wait;
            this.tu = tu;
        }

        @Override
        public void close() throws IOException {
            try {
                future.get(wait, tu);
            } catch ( TimeoutException exc ) { 
                throw new IOException (exc);
            } catch ( ExecutionException exc ) { 
                throw new IOException ( exc);
            } catch ( InterruptedException exc ) { 
                throw new IOException ( exc );
            }
        }
    }

    public static class AppContextCloseable implements Closeable {
        private AbstractApplicationContext m_factory;

        public AppContextCloseable(AbstractApplicationContext factory) {
            super();
            m_factory = factory;
        }

        @Override
        public void close() throws IOException {
            m_factory.close();
        }
    }


    public static class MultipleOutputsCloseable implements Closeable {

        private MultipleOutputs<?, ?> delegate;

        public MultipleOutputsCloseable(MultipleOutputs<?, ?> delegate) {
            super();
            this.delegate = delegate;
        }

        @Override
        public void close() throws IOException {
            try {
                delegate.close();
            } catch (InterruptedException exc) {
                throw new IOException("Inerrupted", exc);
            }
        }
    }

    /**
     * For unshared hbase connections, typically, when a MR job is using
     * {@link TableInputFormat} or {@link TableOutputFormat} and initializes
     * that thru {@link HadoopConnectionFactory#cloneConfiguration()}.
     * 
     * This is awkward pattern new to 0.90 because they say they basically
     * associate a connection with configuration but there's no way to tell if
     * connection was opened in a shared fashion or individual fashion.
     * 
     * Basically, whenever
     * {@link HadoopConnectionFactory#getHBaseConfiguration()} is used for
     * HTable (or HBAdmin) it is always a shared connection. But it's not the
     * case with MR jobs since we want to modify default template and hence we
     * clone it.
     * 
     * @author dmitriy
     * 
     */
    public static class HBaseConnectionCloseable implements Closeable {

        private Configuration m_hbaseConfig;

        public HBaseConnectionCloseable(Configuration hbaseConfig) {
            super();
            m_hbaseConfig = hbaseConfig;
        }

        @Override
        public void close() throws IOException {
            HConnectionManager.deleteConnection(m_hbaseConfig, true);

        }

    }
    
    public static class PoolableHtableCloseable implements Closeable { 
        private HTablePool pool;
        private HTableInterface htable;
        
        public PoolableHtableCloseable(HTablePool pool, HTableInterface htable) {
            super();
            this.pool = pool;
            this.htable = htable;
        }

        @Override
        public void close() throws IOException {
            pool.putTable(htable) ;
        }
    }
    
    public static String fromStream(InputStream is, String encoding) throws IOException {
        StringWriter sw = new StringWriter();
        Reader r = new InputStreamReader(is, "utf-8");
        int ch;
        while (-1 != (ch = r.read()))
            sw.write(ch);
        sw.close();
        return sw.toString();
    }
    
    


}
