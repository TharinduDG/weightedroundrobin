package weightedroundrobin

import scala.collection.mutable
import scala.language.higherKinds
/**
  * Created by tgalappathth on 9/29/17.
  */
object RoundRobinUtils {

  /** Utility function to use as the `syncWithOriginal` parameter in `RoundRobin` trait
    *
    * @param global  globally maintained resource pool
    * @param updated resultant pool after consumed by the resource consumer function
    * @tparam A Result value type
    * @tparam F Any collection type compatible with `mutable.ListBuffer`
    */
  def syncGlobalWithUpdatedResourcePool[A, F[B] <: mutable.Buffer[B], G[C] <: Seq[C]](global: F[A])(updated: G[A]): F[A] = {
    global.indices.foreach(i => {
      val e = updated(i)
      global.update(i, e)
    })
    global
  }

  private def plusNRewarder(reward: Int, weight: Int): Int = {
    weight + reward
  }

  private def divByNPenalizer(penaltyFactor: Int, weight: Int): Int = {
    math.ceil(weight / penaltyFactor).toInt
  }

  def plusOneRewarder(weight: Int): Int = {
    plusNRewarder(reward = 1, weight)
  }

  def plusTwoRewarder(weight: Int): Int = {
    plusNRewarder(reward = 2, weight)
  }

  def divByTwoPenalizer(weight: Int): Int = {
    divByNPenalizer(penaltyFactor = 2, weight)
  }

  def divByFivePenalizer(weight: Int): Int = {
    divByNPenalizer(penaltyFactor = 5, weight)
  }
}
