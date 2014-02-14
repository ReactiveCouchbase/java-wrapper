package org.reactivecouchbase.japi;

import net.spy.memcached.ops.OperationStatus;
import org.reactivecouchbase.ReactiveCouchbaseDriver;
import org.reactivecouchbase.common.Functionnal;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.Json;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.reactivecouchbase.json.Syntax.*;

public class ApiSample1 {

    public static void main(String... args) {
        final ReactiveCouchbaseDriver driver = ReactiveCouchbaseDriver.apply();
        final CouchbaseBucket bucket = new CouchbaseBucket(driver.bucket("default"));
        final ExecutorService ec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        JsObject document = Json.obj(
            $("name", "John"),
            $("surname", "Doe"),
            $("age", 42),
            $("address", Json.obj(
                $("number", 42),
                $("street", "Baker Street"),
                $("city", "London")
            ))
        );
        bucket.set("john-doe", document, FormatHelper.JS_OBJECT_FORMAT).onSuccess(new Functionnal.Action<OperationStatus>() {
            @Override
            public void call(OperationStatus operationStatus) {
                System.out.println("Operation status : " + operationStatus.getMessage());
                driver.shutdown();
            }
        }, ec);
    }
}

