package org.reactivecouchbase.japi;

import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.SpatialView;
import com.couchbase.client.protocol.views.View;
import net.spy.memcached.ops.OperationStatus;
import org.reactivecouchbase.client.Row;
import org.reactivecouchbase.common.Functionnal;
import org.reactivecouchbase.concurrent.Future;
import org.reactivecouchbase.json.Format;
import org.reactivecouchbase.json.JsObject;

import java.util.List;

public class FormatBucket<T> {

    private final CouchbaseBucket bucket;
    private final Format<T> fmt;

    FormatBucket(CouchbaseBucket bucket, Format<T> fmt) {
        this.bucket = bucket;
        this.fmt = fmt;
    }

    public String docName(String name) {
        return bucket.docName(name);
    }

    public Future<List<T>> find(String docName, String viewName, Query query) {
        return bucket.find(docName, viewName, query, fmt);
    }

    public Future<List<T>> find(View view, Query query) {
        return bucket.find(view, query, fmt);
    }

    public Future<List<Row<T>>> search(String docName, String viewName, Query query) {
        return bucket.search(docName, viewName, query, fmt);
    }

    public Future<List<Row<T>>> search(View view, Query query) {
        return bucket.search(view, query, fmt);
    }

    public Future<Functionnal.Option<T>> get(String key) {
        return bucket.get(key, fmt);
    }

    public Future<OperationStatus> set(String key, T value) {
        return bucket.set(key, value, fmt);
    }

    public Future<OperationStatus> add(String key, T value) {
        return bucket.add(key, value, fmt);
    }

    public Future<OperationStatus> replace(String key, T value) {
        return bucket.replace(key, value, fmt);
    }

    public Future<OperationStatus> delete(String key) {
        return bucket.delete(key);
    }

    public Future<View> view(String docName, String view) {
        return bucket.view(docName, view);
    }

    public Future<List<T>> N1QL(String query) {
        return bucket.N1QL(query, fmt);
    }

    public Future<Functionnal.Option<T>> N1QLHeadOption(String query) {
        return bucket.N1QLHeadOption(query, fmt);
    }

    public Future<SpatialView> spatialView(String docName, String viewName) {
        return bucket.spatialView(docName, viewName);
    }

    public Future<DesignDocument> designDocument(String docName) {
        return bucket.designDocument(docName);
    }

    public Future<OperationStatus> createDesignDoc(String name, JsObject value) {
        return bucket.createDesignDoc(name, value);
    }

    public Future<OperationStatus> createDesignDoc(String name, String value) {
        return bucket.createDesignDoc(name, value);
    }

    public Future<OperationStatus> deleteDesignDoc(String name) {
        return bucket.deleteDesignDoc(name);
    }
}
