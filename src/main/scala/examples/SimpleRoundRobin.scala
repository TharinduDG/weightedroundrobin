package examples

import weightedroundrobin.RoundRobinUtils.{divByFivePenalizer, plusOneRewarder}
import weightedroundrobin.{RoundRobin, WeightedResource}

import scala.collection.mutable
import scala.concurrent.Future
import scala.language.higherKinds
import scala.concurrent.ExecutionContext.Implicits.global

object RoundrobinExample {

  private val resourcePool = mutable.ListBuffer(WeightedResource("nr1", "nr1", 100, 75), WeightedResource("nr2", "nr2", 40, 39), WeightedResource("nr3", "nr3", 50, 0))
  private val rr = new RoundRobin[WeightedResource, Seq] {}

  def forResource[A](fn: WeightedResource => A ) : Either[Throwable, A] = {
    val (_, result): (Seq[WeightedResource], Either[Throwable, A]) = rr.forResource(fn)(plusOneRewarder, divByFivePenalizer, resourcePool)(syncSafelyWithResourcePool(resourcePool))
    result
  }

  def forResourceAsync[A](fn: WeightedResource => Future[A] ) : Future[Either[Throwable, A]] = {
    val (_, result): (Future[Seq[WeightedResource]], Future[Either[Throwable, A]]) = rr.forResourceAsync(fn)(plusOneRewarder, divByFivePenalizer, resourcePool)(syncSafelyWithResourcePool(resourcePool))
    result
  }

  private def syncSafelyWithResourcePool[A, F[B] <: mutable.Buffer[B], G[C] <: Seq[C]](global: F[A])(updated: G[A]): F[A] = {
    synchronized {
      global.indices.foreach(i => {
        val e = updated(i)
        global.update(i, e)
      })
    }
    global
  }
}
