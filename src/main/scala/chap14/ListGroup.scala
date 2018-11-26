package chap14

import org.apache.zookeeper.KeeperException

/**
  * Created by zhoudunxiong on 2018/11/26.
  */
class ListGroup extends ConnectionWatcher {

  def listGroup(groupName: String): Unit = {
    import scala.collection.JavaConversions._
    val path = s"/$groupName"
    try {
      val children = zk.getChildren(path, false)
      if (children.isEmpty) {
        println(s"No members in group $groupName\n")
        System.exit(1)
      }
      children.foreach(println)
    } catch {
      case e: KeeperException.NoNodeException =>
        println(s"Group $groupName does not exist\n")
        System.exit(1)
    }
  }
}

object ListGroup {

  def main(args: Array[String]): Unit = {
    val listGroup = new ListGroup()
    listGroup.connect(args(0))
    listGroup.listGroup(args(1))
    listGroup.close()
  }
}
