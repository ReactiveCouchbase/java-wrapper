package org.reactivecouchbase

import scala.concurrent.{ExecutionContext, Future}
import com.couchbase.client.protocol.views._
import org.reactivecouchbase.client.Row
import org.reactivecouchbase.json.Reader
import org.reactivecouchbase.client.CouchbaseFutures._
import scala.collection.JavaConversions._
import java.util
import play.api.libs.json._
import scala.util.Failure
import scala.Some
import scala.util.Success

object FutureHelper {

  def toRCFuture[T](future: Future[T], ec: ExecutionContext): org.reactivecouchbase.japi.concurrent.Future[T] = {
    val jpromise = new org.reactivecouchbase.japi.concurrent.Promise[T]()
    future.onComplete {
      case Success(result) => jpromise.success(result)
      case Failure(e) => jpromise.failure(e)
    }(ec)
    jpromise.future()
  }
}

object ScalaHelper {

  def javaOptGet[T](key: String, reader: Reader[T], bucket: CouchbaseBucket, ec: ExecutionContext): scala.concurrent.Future[org.reactivecouchbase.common.Functionnal.Option[T]] = {
    waitForGet( bucket.couchbaseClient.asyncGet(key), bucket, ec ).map({
      case doc: String => reader.read(org.reactivecouchbase.json.Json.parse(doc)) match {
        case s: org.reactivecouchbase.json.JsSuccess[T] => org.reactivecouchbase.common.Functionnal.Option.some[T](s.get())
        case _ => org.reactivecouchbase.common.Functionnal.Option.none[T]()
      }
      case _ => org.reactivecouchbase.common.Functionnal.Option.none[T]()
    })(ec).asInstanceOf[scala.concurrent.Future[org.reactivecouchbase.common.Functionnal.Option[T]]]
  }

  def javaFind[T](docName:String, viewName: String, query: Query, reader: Reader[T], bucket: CouchbaseBucket, ec: ExecutionContext): scala.concurrent.Future[java.util.List[T]] = {
    Couchbase.view(docName, viewName)(bucket, ec).flatMap { view =>
      javaFind[T](view, query, reader, bucket, ec)
    }(ec)
  }

  def javaFind[T](view: View, query: Query, reader: Reader[T], bucket: CouchbaseBucket, ec: ExecutionContext): scala.concurrent.Future[java.util.List[T]] = {
    waitForHttp( bucket.couchbaseClient.asyncQuery(view, query), bucket, ec ).map { results =>
      new util.ArrayList[T](asJavaCollection(results.iterator().collect {
        case r: ViewRowWithDocs if query.willIncludeDocs() => reader.read(org.reactivecouchbase.json.Json.parse(r.getDocument.asInstanceOf[String]))
        case r: ViewRowReduced if query.willIncludeDocs() => reader.read(org.reactivecouchbase.json.Json.parse(r.getDocument))
        case r: SpatialViewRowWithDocs if query.willIncludeDocs() => reader.read(org.reactivecouchbase.json.Json.parse(r.getDocument.asInstanceOf[String]))
      }.toList.filter(_.isSuccess).map(_.get())))
    }(ec)
  }

  def javaFullFind[T](docName:String, viewName: String, query: Query, reader: Reader[T], bucket: CouchbaseBucket, ec: ExecutionContext): scala.concurrent.Future[java.util.List[Row[T]]] = {
    Couchbase.view(docName, viewName)(bucket, ec).flatMap { view =>
      javaFullFind[T](view, query, reader, bucket, ec)
    }(ec)
  }

  def javaFullFind[T](view: View, query: Query, reader: Reader[T], bucket: CouchbaseBucket, ec: ExecutionContext): scala.concurrent.Future[java.util.List[Row[T]]] = {
    waitForHttp( bucket.couchbaseClient.asyncQuery(view, query), bucket, ec ).map { results =>
      new util.ArrayList[Row[T]](asJavaCollection(results.iterator().map {
        case r: ViewRowWithDocs if query.willIncludeDocs() => new Row[T](Some(reader.read(org.reactivecouchbase.json.Json.parse(r.getDocument.asInstanceOf[String])).get()), r.getId, r.getKey, r.getValue )
        case r: ViewRowNoDocs => new Row[T](d = None, id = r.getId, key = r.getKey, value = r.getValue )
        case r: ViewRowReduced if query.willIncludeDocs() => new Row[T](Some(reader.read(org.reactivecouchbase.json.Json.parse(r.getDocument)).get()), "", r.getKey, r.getValue )
        case r: ViewRowReduced if !query.willIncludeDocs() => new Row[T](d = None, id = "", key = r.getKey, value = r.getValue )
        case r: SpatialViewRowNoDocs => new Row[T](d = None, id = r.getId, key = r.getKey, value = r.getValue )
        case r: SpatialViewRowWithDocs if query.willIncludeDocs() => new Row[T](Some(reader.read(org.reactivecouchbase.json.Json.parse(r.getDocument.asInstanceOf[String])).get()), r.getId, r.getKey, r.getValue )
        case r: SpatialViewRowWithDocs if !query.willIncludeDocs() => new Row[T](d = None, id = r.getId, key = r.getKey, value = r.getValue )
      }.toList))
    }(ec)
  }

  def n1qlSearch[T](query: String, reader: Reader[T], bucket: CouchbaseBucket, ec: ExecutionContext): scala.concurrent.Future[java.util.List[T]] = {
    CouchbaseN1QL.N1QL(query)(bucket).toList(new Reads[T] {
      def reads(json: JsValue): JsResult[T] = new JsSuccess[T](reader.read(org.reactivecouchbase.json.Json.parse(Json.stringify(json))).get())
    }, ec).map(l => new java.util.ArrayList[T](asJavaCollection(l)))(ec)
  }

  def n1qlHeadOption[T](query: String, reader: Reader[T], bucket: CouchbaseBucket, ec: ExecutionContext): scala.concurrent.Future[org.reactivecouchbase.common.Functionnal.Option[T]] = {
    CouchbaseN1QL.N1QL(query)(bucket).headOption(new Reads[T] {
      def reads(json: JsValue): JsResult[T] = new JsSuccess[T](reader.read(org.reactivecouchbase.json.Json.parse(Json.stringify(json))).get())
    }, ec).map {
      case Some(head) => org.reactivecouchbase.common.Functionnal.Option.some[T](head)
      case None => org.reactivecouchbase.common.Functionnal.Option.none[T]()
    }(ec)
  }

}