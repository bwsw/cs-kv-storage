import akka.actor.ActorSystem
import com.bwsw.kv.storage._
import com.bwsw.kv.storage.app.Configuration
import com.bwsw.kv.storage.manager.ElasticsearchKvStorageManager
import com.bwsw.kv.storage.processor.ElasticsearchKvProcessor
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import javax.servlet.ServletContext
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {
  val conf = new Configuration
  val client = HttpClient(ElasticsearchClientUri(conf.getElasticsearchUri))
  val processor = new ElasticsearchKvProcessor(client, conf)
  val manager = new ElasticsearchKvStorageManager(client)
  val system = ActorSystem()

  override def init(context: ServletContext) {
    context.mount(new KvStorageManagerServlet(system, manager), "/*")
    context.mount(new KvStorageServlet(system, processor), "/*")
  }

  override def destroy(context: ServletContext) {
    system.terminate
  }
}
