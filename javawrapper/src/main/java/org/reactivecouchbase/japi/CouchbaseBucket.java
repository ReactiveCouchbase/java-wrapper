package org.reactivecouchbase.japi;

import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.google.common.base.Function;
import com.sun.istack.internal.Nullable;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.ops.OperationStatus;
import org.reactivecouchbase.Couchbase$;
import org.reactivecouchbase.CouchbaseN1QL$;
import org.reactivecouchbase.FutureHelper$;
import org.reactivecouchbase.ScalaHelper$;
import org.reactivecouchbase.client.Row;
import org.reactivecouchbase.common.Functionnal;
import org.reactivecouchbase.japi.concurrent.Future;
import org.reactivecouchbase.json.Format;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.Reader;
import org.reactivecouchbase.json.Writer;
import scala.collection.JavaConversions$;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CouchbaseBucket {

    private final org.reactivecouchbase.CouchbaseBucket bucket;

    private final Integer expirationMillis;
    private final PersistTo persistTo;
    private final ReplicateTo replicateTo;
    private final ExecutorService es;
    private final ExecutionContext ec;
    private final Couchbase$ couchbase;

    public CouchbaseBucket(org.reactivecouchbase.CouchbaseBucket currentBucket) {
        this.bucket = currentBucket;
        this.expirationMillis = -1;
        this.persistTo = PersistTo.ZERO;
        this.replicateTo = ReplicateTo.ZERO;
        this.es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.ec = ExecutionContext$.MODULE$.fromExecutorService(es);
        this.couchbase = Couchbase$.MODULE$;
    }

    public CouchbaseBucket(org.reactivecouchbase.CouchbaseBucket currentBucket, ExecutorService ec) {
        this.bucket = currentBucket;
        this.expirationMillis = -1;
        this.persistTo = PersistTo.ZERO;
        this.replicateTo = ReplicateTo.ZERO;
        this.es = ec;
        this.ec = ExecutionContext$.MODULE$.fromExecutorService(es);
        this.couchbase = Couchbase$.MODULE$;
    }

    public CouchbaseBucket(org.reactivecouchbase.CouchbaseBucket currentBucket, Integer expirationMillis, PersistTo persistTo, ReplicateTo replicateTo, ExecutorService ec) {
        this.bucket = currentBucket;
        this.expirationMillis = expirationMillis;
        this.persistTo = persistTo;
        this.replicateTo = replicateTo;
        this.es = ec;
        this.ec = ExecutionContext$.MODULE$.fromExecutorService(es);
        this.couchbase = Couchbase$.MODULE$;
    }

    public CouchbaseBucket withExecutor(ExecutorService ec) {
        return new CouchbaseBucket(bucket, expirationMillis, persistTo, replicateTo, ec);
    }

    public CouchbaseBucket withExpiration(Integer value) {
        return new CouchbaseBucket(bucket, value, persistTo, replicateTo, es);
    }

    public CouchbaseBucket withPersistTo(PersistTo value) {
        return new CouchbaseBucket(bucket, expirationMillis, value, replicateTo, es);
    }

    public CouchbaseBucket withReplicateTo(ReplicateTo value) {
        return new CouchbaseBucket(bucket, expirationMillis, persistTo, replicateTo, es);
    }

    public CouchbaseBucket withClusterOpts(PersistTo value1, ReplicateTo value2) {
        return new CouchbaseBucket(bucket, expirationMillis, value1, value2, es);
    }

    public <T> FormatBucket<T> withFormat(Format<T> fmt) {
        return new FormatBucket<T>(this, fmt);
    }

    public <T> Future<List<T>> find(String docName, String viewName, Query query, Reader<T> reader) {
        return FutureHelper$.MODULE$.toRCFuture(ScalaHelper$.MODULE$.javaFind(docName, viewName, query, reader, bucket, ec), ec);
    }

    public <T> Future<List<T>> find(View view, Query query, Reader<T> reader) {
        return FutureHelper$.MODULE$.toRCFuture(ScalaHelper$.MODULE$.javaFind(view, query, reader, bucket, ec), ec);
    }

    public <T> Future<List<Row<T>>> search(String docName, String viewName, Query query, Reader<T> reader) {
        return FutureHelper$.MODULE$.toRCFuture(ScalaHelper$.MODULE$.javaFullFind(docName, viewName, query, reader, bucket, ec), ec);
    }

    public <T> Future<List<Row<T>>> search(View view, Query query, Reader<T> reader) {
        return FutureHelper$.MODULE$.toRCFuture(ScalaHelper$.MODULE$.javaFullFind(view, query, reader, bucket, ec), ec);
    }

    public Future<View> view(String docName, String view) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.javaView(docName, view, bucket, ec), ec);
    }

    public <T> Future<Functionnal.Option<T>> get(String key, Reader<T> reader) {
        return FutureHelper$.MODULE$.toRCFuture(ScalaHelper$.MODULE$.javaOptGet(key, reader, bucket, ec), ec);
    }

    public Future<OperationStatus> incr(String key, Integer of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.incr(key, of, bucket, ec), ec);
    }

    public Future<OperationStatus> incr(String key, Long of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.incr(key, of, bucket, ec), ec);
    }

    public Future<OperationStatus> decr(String key, Integer of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.decr(key, of, bucket, ec), ec);
    }

    public Future<OperationStatus> decr(String key, Long of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.decr(key, of, bucket, ec), ec);
    }

    public Future<Integer> incrAndGet(String key, Integer of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.asJavaInt(couchbase.incrAndGet(key, of, bucket, ec), ec), ec);
    }

    public Future<Long> incrAndGet(String key, Long of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.asJavaLong(couchbase.incrAndGet(key, of, bucket, ec), ec), ec);
    }

    public Future<Integer> decrAndGet(String key, Integer of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.asJavaInt(couchbase.decrAndGet(key, of, bucket, ec), ec), ec);
    }

    public Future<Long> decrAndGet(String key, Long of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.asJavaLong(couchbase.decrAndGet(key, of, bucket, ec), ec), ec);
    }

    public <T> Future<OperationStatus> set(String key, T value, Writer<T> fmt) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.javaSet(key, expirationMillis, Json.stringify(fmt.write(value)), persistTo, replicateTo, bucket, ec), ec);
    }

    public <T> Future<OperationStatus> add(String key, T value, Writer<T> fmt) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.javaAdd(key, expirationMillis, Json.stringify(fmt.write(value)), persistTo, replicateTo, bucket, ec), ec);
    }

    public <T> Future<OperationStatus> replace(String key, T value, Writer<T> fmt) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.javaReplace(key, expirationMillis, Json.stringify(fmt.write(value)), persistTo, replicateTo, bucket, ec), ec);
    }

    public Future<OperationStatus> delete(String key){
        return FutureHelper$.MODULE$.toRCFuture(couchbase.delete(key, persistTo, replicateTo, bucket, ec), ec);
    }

    public Future<OperationStatus> flush(int delay) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.flush(delay, bucket, ec), ec);
    }

    public Future<OperationStatus> flush() {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.flush(bucket, ec), ec);
    }

    public <T> Future<List<T>> N1QL(String query, Reader<T> reader) {
        return FutureHelper$.MODULE$.toRCFuture(ScalaHelper$.MODULE$.n1qlSearch(query, reader, bucket, ec), ec);
    }

    // TODO : design doc mgmt
    // TODO : atomic update support
    // TODO : crud support
    // TODO : rewrite play java api
}
