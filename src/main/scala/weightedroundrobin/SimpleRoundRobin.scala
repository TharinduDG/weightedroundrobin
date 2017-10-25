package weightedroundrobin

import weightedroundrobin.RoundRobinUtils._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.higherKinds
import scala.reflect.runtime.universe._

/**
  * Created by tgalappathth on 10/2/17.
  */
class SimpleRoundRobin[R <: Resource, F[B] <: mutable.Buffer[B]](val resourcePool: F[R]) {

  private val rr = new RoundRobin[R, mutable.Buffer] {}

  def forResource[S](fn: (R) => S)(implicit tag: TypeTag[R]): Either[Throwable, S] = {
    val (_, result) = rr.forResource(fn)(plusOneRewarder, divByFivePenalizer, resourcePool)(syncGlobalWithUpdatedResourcePool(resourcePool))
    result
  }

  def forResourceAsync[S](fn: (R) => Future[S])(implicit tag: TypeTag[R]): Future[Either[Throwable, S]] = {
    val (_, result) = rr.forResourceAsync(fn)(plusOneRewarder, divByFivePenalizer, resourcePool)(syncGlobalWithUpdatedResourcePool(resourcePool))
    result
  }
}
