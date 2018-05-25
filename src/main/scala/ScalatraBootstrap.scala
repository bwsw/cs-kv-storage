import akka.actor.ActorSystem
import com.bwsw.kv.storage._
import com.bwsw.kv.storage.app.Configuration
import com.bwsw.kv.storage.processor.ElasticsearchKvProcessor
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import org.scalatra._
import javax.servlet.ServletContext

class ScalatraBootstrap extends LifeCycle {
  val Conf = new Configuration
  val Client = HttpClient(ElasticsearchClientUri(Conf.getElasticsearchUri))
  val Processor = new ElasticsearchKvProcessor(Client, Conf)
  val System = ActorSystem()

  override def init(context: ServletContext) {
    context.mount(new KvStorageServlet(System, Processor), "/*")
  }

  override def destroy(context:ServletContext) {
    System.terminate
  }
}
