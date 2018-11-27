package chap14

import org.apache.zookeeper.KeeperException

/**
  * Created by zhoudunxiong on 2018/11/27.
  */
class DeleteGroup extends ConnectionWatcher {

  def delete(groupName: String): Unit = {
    import scala.collection.JavaConversions._
    val path = "/" + groupName
    try {
      val children = zk.getChildren(path, false)
      children.foreach(c => zk.delete(path + "/" + c, -1))
      zk.delete(path, -1)
    } catch {
      case e: KeeperException.NoNodeException =>
        println(s"Group $groupName does not exist \n")
        System.exit(-1)
    }
  }
}

object DeleteGroup {

  def main(args: Array[String]): Unit = {
    val deleteGroup = new DeleteGroup()
    deleteGroup.connect(args(0))
    deleteGroup.delete(args(1))
    deleteGroup.close()
  }
}
