package org.reactivecouchbase.japi;

import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.SpatialView;
import com.couchbase.client.protocol.views.View;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.ops.OperationStatus;
import org.reactivecouchbase.Couchbase$;
import org.reactivecouchbase.FutureHelper$;
import org.reactivecouchbase.ScalaHelper$;
import org.reactivecouchbase.client.OpResult;
import org.reactivecouchbase.client.Row;
import org.reactivecouchbase.common.Functionnal;
import org.reactivecouchbase.concurrent.Future;
import org.reactivecouchbase.json.*;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CouchbaseBucket {

    final org.reactivecouchbase.CouchbaseBucket bucket;

    private final Integer expirationMillis;
    private final PersistTo persistTo;
    private final ReplicateTo replicateTo;
    private final ExecutorService es;
    private final ExecutionContext ec;
    private final Couchbase$ couchbase;

    public CouchbaseBucket(org.reactivecouchbase.CouchbaseBucket currentBucket) {
        this.bucket = currentBucket;
        this.expirationMillis = 0;
        this.persistTo = PersistTo.ZERO;
        this.replicateTo = ReplicateTo.ZERO;
        this.es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.ec = ExecutionContext$.MODULE$.fromExecutorService(es);
        this.couchbase = Couchbase$.MODULE$;
    }

    public CouchbaseBucket(org.reactivecouchbase.CouchbaseBucket currentBucket, ExecutorService ec) {
        this.bucket = currentBucket;
        this.expirationMillis = 0;
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

    public String docName(String name) {
        return bucket.cbDriver().mode().name() + name;
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

    public Future<Integer> incr(String key, Integer of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.incr(key, of, bucket, ec), ec).mapTo(Integer.class);
    }

    public Future<Long> incr(String key, Long of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.incr(key, of, bucket, ec), ec).mapTo(Long.class);
    }

    public Future<Integer> decr(String key, Integer of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.decr(key, of, bucket, ec), ec).mapTo(Integer.class);
    }

    public Future<Long> decr(String key, Long of) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.decr(key, of, bucket, ec), ec).mapTo(Long.class);
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

    public <T> Future<OpResult> set(String key, T value, Writer<T> fmt) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.javaSet(key, expirationMillis, Json.stringify(fmt.write(value)), persistTo, replicateTo, bucket, ec), ec);
    }

    public <T> Future<OpResult> add(String key, T value, Writer<T> fmt) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.javaAdd(key, expirationMillis, Json.stringify(fmt.write(value)), persistTo, replicateTo, bucket, ec), ec);
    }

    public <T> Future<OpResult> replace(String key, T value, Writer<T> fmt) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.javaReplace(key, expirationMillis, Json.stringify(fmt.write(value)), persistTo, replicateTo, bucket, ec), ec);
    }

    public Future<OpResult> delete(String key){
        return FutureHelper$.MODULE$.toRCFuture(couchbase.delete(key, persistTo, replicateTo, bucket, ec), ec);
    }

    public Future<OpResult> flush(int delay) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.flush(delay, bucket, ec), ec);
    }

    public Future<OpResult> flush() {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.flush(bucket, ec), ec);
    }

    public <T> Future<List<T>> N1QL(String query, Reader<T> reader) {
        return FutureHelper$.MODULE$.toRCFuture(ScalaHelper$.MODULE$.n1qlSearch(query, reader, bucket, ec), ec);
    }

    public <T> Future<Functionnal.Option<T>> N1QLHeadOption(String query, Reader<T> reader) {
        return FutureHelper$.MODULE$.toRCFuture(ScalaHelper$.MODULE$.n1qlHeadOption(query, reader, bucket, ec), ec);
    }

    public Future<SpatialView> spatialView(String docName, String viewName) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.spatialView(docName, viewName, bucket, ec), ec);
    }

    public Future<DesignDocument> designDocument(String docName) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.designDocument(docName, bucket, ec), ec);
    }

    public Future<OpResult> createDesignDoc(String name, JsObject value) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.createDesignDoc(name, Json.stringify(value), bucket, ec), ec);
    }

    public Future<OpResult> createDesignDoc(String name, String value) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.createDesignDoc(name, value, bucket, ec), ec);
    }

    public Future<OpResult> deleteDesignDoc(String name) {
        return FutureHelper$.MODULE$.toRCFuture(couchbase.deleteDesignDoc(name, bucket, ec), ec);
    }

    // TODO : atomic update support
    // TODO : crud support
    // TODO : rewrite play java api
}
