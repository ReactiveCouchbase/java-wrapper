package org.reactivecouchbase.japi;

import org.reactivecouchbase.json.*;

public class FormatHelper {

    public static final Format<JsValue> JS_VALUE_FORMAT = new Format<JsValue>() {
        @Override
        public JsResult<JsValue> read(JsValue value) {
            return new JsSuccess<JsValue>(value);
        }

        @Override
        public JsValue write(JsValue value) {
            return value;
        }
    };

    public static final Format<JsObject> JS_OBJECT_FORMAT = new Format<JsObject>() {
        @Override
        public JsResult<JsObject> read(JsValue value) {
            return new JsSuccess<JsObject>(value.as(JsObject.class));
        }

        @Override
        public JsValue write(JsObject value) {
            return value;
        }
    };
}
