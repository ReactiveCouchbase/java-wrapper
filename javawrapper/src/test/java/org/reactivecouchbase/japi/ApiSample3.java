package org.reactivecouchbase.japi;

import com.google.common.base.Function;
import com.sun.istack.internal.Nullable;
import net.spy.memcached.ops.OperationStatus;
import org.reactivecouchbase.ReactiveCouchbaseDriver;
import org.reactivecouchbase.common.Functionnal;
import org.reactivecouchbase.json.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.reactivecouchbase.json.JsResult.combine;
import static org.reactivecouchbase.json.Syntax.$;

public class ApiSample3 {

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

        @Override
        public String toString() {
            return "Address{" +
                    "number='" + number + '\'' +
                    ", street='" + street + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
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

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", surname='" + surname + '\'' +
                    ", age=" + age +
                    ", address=" + address +
                    '}';
        }
    }

    public static void main(String... args) {
        final ReactiveCouchbaseDriver driver = ReactiveCouchbaseDriver.apply();
        final CouchbaseBucket bucket = new CouchbaseBucket(driver.bucket("default"));
        final ExecutorService ec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        // get the Person instance with the key 'john-doe', using implicit 'personFmt' for deserialization
        bucket.get("john-doe", Person.FORMAT).map(new Function<Functionnal.Option<Person>, Functionnal.Unit>() {
            @Override
            public Functionnal.Unit apply(Functionnal.Option<Person> input) {
                System.out.println(
                    input.map(new Function<Person, String>() {
                        @Override
                        public String apply(Person input) {
                            return "Found John : " + input;
                        }
                    }).getOrElse("Cannot find object with key 'john-doe'")
                );
                driver.shutdown();
                return Functionnal.Unit.unit();
            }
        }, ec);
    }
}

