package org.reactivecouchbase.japi;

import com.google.common.base.Function;
import net.spy.memcached.ops.OperationStatus;
import org.reactivecouchbase.ReactiveCouchbaseDriver;
import org.reactivecouchbase.common.Functionnal;
import org.reactivecouchbase.json.Format;
import org.reactivecouchbase.json.JsResult;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.reactivecouchbase.json.JsResult.combine;
import static org.reactivecouchbase.json.Syntax.$;

public class ApiSample2 {

    public static class Address {
        public final String number;
        public final String street;
        public final String city;
        public Address(String number, String street, String city) {
            this.number = number;
            this.street = street;
            this.city = city;
        }
        public static final Format<Address> FORMAT =  new Format<Address>() {
            @Override
            public JsResult<Address> read(JsValue value) {
                return combine(
                    value.field("number").read(String.class),
                    value.field("street").read(String.class),
                    value.field("city").read(String.class)
                ).map(new Function<Functionnal.T3<String, String, String>, Address>() {
                    @Override
                    public Address apply(Functionnal.T3<String, String, String> input) {
                        return new Address(input._1, input._2, input._3);
                    }
                });
            }
            @Override
            public JsValue write(Address value) {
                return Json.obj(
                    $("number", value.number),
                    $("street", value.street),
                    $("city", value.city)
                );
            }
        };
    }

    public static class Person {
        public final String name;
        public final String surname;
        public final Integer age;
        public final Address address;
        public Person(String name, String surname, Integer age, Address address) {
            this.name = name;
            this.surname = surname;
            this.age = age;
            this.address = address;
        }

        public static final Format<Person> FORMAT = new Format<Person>() {
            @Override
            public JsResult<Person> read(JsValue value) {
                return combine(
                    value.field("name").read(String.class),
                    value.field("surname").read(String.class),
                    value.field("age").read(Integer.class),
                    value.field("address").read(Address.FORMAT)
                ).map(new Function<Functionnal.T4<String, String, Integer, Address>, Person>() {
                    @Override
                    public Person apply(Functionnal.T4<String, String, Integer, Address> input) {
                        return new Person(input._1, input._2, input._3, input._4);
                    }
                });
            }
            @Override
            public JsValue write(Person value) {
                return Json.obj(
                    $("name", value.name),
                    $("surname", value.surname),
                    $("age", value.age),
                    $("address", Address.FORMAT.write(value.address))
                );
            }
        };
    }

    public static void main(String... args) {
        final ReactiveCouchbaseDriver driver = ReactiveCouchbaseDriver.apply();
        final CouchbaseBucket bucket = new CouchbaseBucket(driver.bucket("default"));
        final ExecutorService ec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        Person document = new Person("John", "Doe", 42, new Address("221b", "Baker Street", "London"));

        // persist the Person instance with the key 'john-doe', using implicit 'personFmt' for serialization
        bucket.set("john-doe", document, Person.FORMAT).onSuccess(new Functionnal.Action<OperationStatus>() {
            @Override
            public void call(OperationStatus operationStatus) {
                System.out.println("Operation status : " + operationStatus.getMessage());
                driver.shutdown();
            }
        }, ec);

    }
}

