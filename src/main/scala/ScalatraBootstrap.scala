import akka.actor.ActorSystem
import com.bwsw.kv.storage._
import com.bwsw.kv.storage.app.Configuration
import com.bwsw.kv.storage.manager.ElasticsearchKvStorageManager
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import org.scalatra._
import javax.servlet.ServletContext

class ScalatraBootstrap extends LifeCycle {
  val conf = new Configuration
  val client = HttpClient(ElasticsearchClientUri(conf.getElasticsearchUri))
  val manager = new ElasticsearchKvStorageManager(client)
  val system = ActorSystem()
  override def init(context: ServletContext) {
    context.mount(new KvStorageManagerServlet(system, manager), "/*")
  }
}
