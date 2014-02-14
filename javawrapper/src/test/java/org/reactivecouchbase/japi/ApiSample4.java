package org.reactivecouchbase.japi;

import com.couchbase.client.protocol.views.Query;
import com.google.common.base.Function;
import com.sun.istack.internal.Nullable;
import net.spy.memcached.ops.OperationStatus;
import org.reactivecouchbase.ReactiveCouchbaseDriver;
import org.reactivecouchbase.client.Row;
import org.reactivecouchbase.common.Functionnal;
import org.reactivecouchbase.japi.concurrent.Future;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.Json;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.reactivecouchbase.json.Syntax.$;

public class ApiSample4 {

    public static void main(String... args) {
        final ReactiveCouchbaseDriver driver = ReactiveCouchbaseDriver.apply();
        final CouchbaseBucket bucket = new CouchbaseBucket(driver.bucket("default"));
        final ExecutorService ec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        // search the whole list of docs from the view 'brewery_beers'
        Future<List<Row<JsObject>>> futureList =
            bucket.search("beers", "brewery_beers", new Query().setIncludeDocs(true), FormatHelper.JS_OBJECT_FORMAT);

        // when the query is done, run over all the docs and print them
        futureList.map(new Function<List<Row<JsObject>>, Functionnal.Unit>() {
            @Override
            public Functionnal.Unit apply(List<Row<JsObject>> input) {
                for (Row<JsObject> row : input) {
                    System.out.println(Json.prettyPrint(row.docOpt().get()));
                }
                driver.shutdown();
                return Functionnal.Unit.unit();
            }
        }, ec);
    }
}

