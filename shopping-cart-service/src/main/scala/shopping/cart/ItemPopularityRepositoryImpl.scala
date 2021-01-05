package shopping.cart

import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import scala.concurrent.ExecutionContext

object ItemPopularityRepositoryImpl {
  val popularityTable = "item_popularity"
}

class ItemPopularityRepositoryImpl(session: CassandraSession, keySpace: String)(
    implicit val ec: ExecutionContext)
    extends ItemPopularityRepository {

  import ItemPopularityRepositoryImpl.popularityTable

  override def update(itemId: String, delta: Int) =
    session.executeWrite(
      s"update $keySpace.$popularityTable set count = count + ? where item_id = ?",
      java.lang.Long.valueOf(delta),
      itemId)

  override def getItem(itemId: String) =
    session
      .selectOne(s"select item_id, count from $keySpace.$popularityTable where item_id = ?", itemId)
      .map(_.map(_.getLong("count").longValue))
}
