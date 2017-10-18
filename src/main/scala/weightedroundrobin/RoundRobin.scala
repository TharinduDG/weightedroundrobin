package weightedroundrobin

import cats.data.State

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.control.NonFatal
import scala.reflect.runtime.universe._

/**
  * Created by Tharindu Galappaththi on 9/29/17.
  */
trait RoundRobin[R <: Resource, F[B] <: Seq[B]] {

  /** Executes the given resource consumer function `fn` and returns the result of `fn` or the exception
    * along with the updated resource pool. Causes side effects using `syncWithOriginal`.
    * There's a risk of thread safety of when consuming the resource pool.
    * Could result in lost updates for resources.
    *
    * @param fn                    Resource Consumer Function
    * @param rewarder              Function to reward the resource when it is successfully consumed
    * @param penalizer             Function to penalize the resource when it's consumption failed
    * @param resourcePool          Global Resources pool to be consumed
    * @param syncGlobalWithUpdated Function to execute after the resource has been consumed by the `fn`
    * @param tag                   Implicit Type tag to identify Resource
    * @tparam S Return type of the resource consumer function `fn`
    * @return a `Tuple2` of updated `resourcePool` with an `Either` of result or the occurred exception
    */
  def forResourceUnsafe[S](fn: (R) => S)
                          (rewarder: Int => Int, penalizer: Int => Int, resourcePool: F[R])(syncGlobalWithUpdated: F[R] => F[R])
                          (implicit tag: TypeTag[R]): (F[R], Either[Throwable, S]) = {

    val state: State[F[R], Either[Throwable, S]] = resourceConsumer(fn)(rewarder, penalizer).modify(s => {
      syncGlobalWithUpdated(s)
    })

    state.run(resourcePool).value
  }

  /** Executes the given resource consumer function `fn` and returns the result of `fn` or the exception
    * along with the updated resource pool. Causes side effects using `syncWithOriginal`.
    * `resourcePool` consumption is thread safe.
    *
    * @param fn                    Resource Consumer Function
    * @param rewarder              Function to reward the resource when it is successfully consumed
    * @param penalizer             Function to penalize the resource when it's consumption failed
    * @param resourcePool          Global Resources pool to be consumed
    * @param syncGlobalWithUpdated Function to execute after the resource has been consumed by the `fn`
    * @param tag                   Implicit Type tag to identify Resource
    * @tparam S Return type of the resource consumer function `fn`
    * @return a `Tuple2` of updated `resourcePool` with an `Either` of result or the occurred exception
    */
  def forResource[S](fn: (R) => S)
                    (rewarder: Int => Int, penalizer: Int => Int, resourcePool: F[R])(syncGlobalWithUpdated: F[R] => F[R])
                    (implicit tag: TypeTag[R]): (F[R], Either[Throwable, S]) = {

    val state = resourceConsumer(fn)(rewarder, penalizer).modify(l => {
      syncGlobalWithUpdated(l)
    })

    val result = synchronized {
      state.run(resourcePool).value
    }
    result
  }

  /** Executes the given resource consumer function `fn` and returns the result of `fn` or the exception
    * along with the updated resource pool. Causes side effects using `syncWithOriginal`.
    * There's a risk of thread safety of when consuming the resource pool.
    * Could result in lost updates for resources.
    *
    * @param fn                    Resource Consumer Function that returns a `Future`
    * @param rewarder              Function to reward the resource when it is successfully consumed
    * @param penalizer             Function to penalize the resource when it's consumption failed
    * @param resourcePool          Global Resources pool to be consumed
    * @param syncGlobalWithUpdated Function to execute after the resource has been consumed by the `fn`
    * @param tag                   Implicit Type tag to identify Resource
    * @tparam S type of the `Future` that the resource consumer function `fn` will return
    * @return a `Tuple2` of updated `resourcePool` with an `Either` of result or the occurred exception
    */
  def forResourceAsyncUnsafe[S](fn: (R) => Future[S])
                               (rewarder: Int => Int, penalizer: Int => Int, resourcePool: F[R])(syncGlobalWithUpdated: F[R] => F[R])
                               (implicit executionContext: ExecutionContext, tag: TypeTag[R]): (Future[F[R]], Future[Either[Throwable, S]]) = {

    val state = resourceConsumerAsync(fn)(rewarder, penalizer).modify(f => {
      f.map(t => {
        syncGlobalWithUpdated(t)
      })
    })

    state.run(Future.successful[F[R]](resourcePool)).value
  }

  /** Executes the given resource consumer function `fn` and returns the result of `fn` or the exception
    * along with the updated resource pool. Causes side effects using `syncWithOriginal`.
    * `resoucePool` consumption is thread safe.
    *
    * @param fn                    Resource Consumer Function that returns a `Future`
    * @param rewarder              Function to reward the resource when it is successfully consumed
    * @param penalizer             Function to penalize the resource when it's consumption failed
    * @param resourcePool          Global Resources pool to be consumed
    * @param syncGlobalWithUpdated Function to execute after the resource has been consumed by the `fn`
    * @param tag                   Implicit Type tag to identify Resource
    * @tparam S type of the `Future` that the resource consumer function `fn` will return
    * @return a `Tuple2` of updated `resourcePool` with an `Either` of result or the occurred exception
    */
  def forResourceAsync[S](fn: (R) => Future[S])
                         (rewarder: Int => Int, penalizer: Int => Int, resourcePool: F[R])(syncGlobalWithUpdated: F[R] => F[R])
                         (implicit executionContext: ExecutionContext, tag: TypeTag[R]): (Future[F[R]], Future[Either[Throwable, S]]) = {

    val state = resourceConsumerAsync(fn)(rewarder, penalizer).modify(f => {
      f.map(t => {
        syncGlobalWithUpdated(t)
      })
    })

    val result = synchronized {
      state.run(Future.successful[F[R]](resourcePool)).value
    }

    result
  }

  /** Pure Function which Produces a `State` monad to consume any `Resource` pool using the given function `fn`
    *
    * @param fn        Function to consume a resource of type `R`
    * @param rewarder  Function to reward the resource when it is successfully consumed
    * @param penalizer Function to penalize the resource when it's consumption failed
    * @param tag       Implicit Type tag to identify Resource
    * @tparam S type of the result that the resource consumer function `fn` will return
    * @return a `cats.data.State` monad that produces a `Tuple2` of updated resource pool and the result/exception
    */
  def resourceConsumer[S](fn: (R) => S)(rewarder: Int => Int, penalizer: Int => Int)
                         (implicit tag: TypeTag[R]): State[F[R], Either[Throwable, S]] = {

    val state = State[F[R], Either[Throwable, S]] {
      pool => consumeResourceCandidate(fn)(pool, rewarder, penalizer)
    }

    state
  }

  /** Pure Function which Produces a `State` monad to consume any `Resource` pool using the given function `fn`
    *
    * @param fn        Function to consume a resource of type `R`
    * @param rewarder  Function to reward the resource when it is successfully consumed
    * @param penalizer Function to penalize the resource when it's consumption failed
    * @param tag       Implicit Type tag to identify Resource
    * @tparam S type of the `Future` that the resource consumer function `fn` will return
    * @return a `cats.data.State` monad that produces a `Tuple2` of updated resource pool and the result/exception
    */
  def resourceConsumerAsync[S](fn: (R) => Future[S])(rewarder: Int => Int, penalizer: Int => Int)
                              (implicit executionContext: ExecutionContext, tag: TypeTag[R]): State[Future[F[R]], Future[Either[Throwable, S]]] = {

    val state = State[Future[F[R]], Future[Either[Throwable, S]]] {
      poolF => {
        val consumedPoolAndExecution = poolF.flatMap(p => consumeResourceCandidateAsync(fn)(p, rewarder, penalizer))

        val consumedPool = consumedPoolAndExecution.flatMap(_._1)
        val execution = consumedPoolAndExecution.map(_._2)

        (consumedPool, execution)
      }
    }

    state
  }

  /** Actual modifier logic for the `State` monad
    *
    * @param fn           Function to consume a resource of type `R`
    * @param resourcePool collection of resources to be consumed
    * @param rewarder     Function to reward the resource when it is successfully consumed
    * @param penalizer    Function to penalize the resource when it's consumption failed
    * @param tag          type of the result that the resource consumer function `fn` will return
    * @tparam S type of the result that the resource consumer function `fn` will return
    * @return Tuple with consumed pool and exception/result
    */
  private def consumeResourceCandidate[S](fn: (R) => S)(resourcePool: F[R], rewarder: Int => Int, penalizer: Int => Int)
                                         (implicit tag: TypeTag[R]): (F[R], Either[Throwable, S]) = {

    val (candidate, rewardedPool) = updateResource(resourcePool, rewarder)

    try {
      val result = fn(candidate)

      (rewardedPool, Right(result))
    } catch {
      case NonFatal(ex) =>
        val (_, penalizedPool) = updateResource(resourcePool, penalizer)

        (penalizedPool, Left(ex))
    }
  }

  /** Actual modifier logic for the `State` monad
    *
    * @param fn           Function to consume a resource of type `R`
    * @param resourcePool collection of resources to be consumed
    * @param rewarder     Function to reward the resource when it is successfully consumed
    * @param penalizer    Function to penalize the resource when it's consumption failed
    * @param tag          type of the result that the resource consumer function `fn` will return
    * @tparam S type of the result that the resource consumer function `fn` will return
    * @return Tuple with consumed pool and exception/result
    */
  private def consumeResourceCandidateAsync[S](fn: (R) => Future[S])(resourcePool: F[R], rewarder: Int => Int, penalizer: Int => Int)
                                              (implicit executionContext: ExecutionContext, tag: TypeTag[R]): Future[(Future[F[R]], Either[Throwable, S])] = {

    val (candidate, rewardedPool) = updateResource(resourcePool, rewarder)

    val triggeredFn = fn(candidate)

    val triggeredTask: Future[Either[(Throwable, F[R]), (S, F[R])]] = triggeredFn.map(v => {
      Right((v, rewardedPool))

    }).recoverWith({
      case NonFatal(ex) =>
        val (_, penalizedPool) = updateResource(resourcePool, penalizer)
        Future.successful(Left((ex, penalizedPool)))
    })

    triggeredTask.map({
      case Left((ex, pool)) => (Future.successful(pool), Left(ex))
      case Right((v, pool)) => (Future.successful(pool), Right(v))
    })
  }

  /** Update the resource based on its type and weight
    *
    * @param resourcePool      collection of resources to be consumed
    * @param weightManipulator Function to update weight of the resource
    * @param tag               type of the result that the resource consumer function `fn` will return
    * @tparam A Type of the Resource
    * @return tuple of consumable resource with the resultant pool
    */
  private def updateResource[A](resourcePool: F[A], weightManipulator: Int => Int)(implicit tag: TypeTag[R]): (A, F[A]) = {

    typeOf(tag) match {
      case t if t =:= typeOf[NeutralResource] =>
        val head = resourcePool.head
        val alteredPool = resourcePool.tail :+ head

        (head, alteredPool.asInstanceOf[F[A]])

      case t if t =:= typeOf[WeightedResource] =>
        val casted: Seq[WeightedResource] = resourcePool.asInstanceOf[Seq[WeightedResource]]
        val sortedResources = casted.sorted(Ordering.by[WeightedResource, Int](_.currentWeight).reverse)
        val candidate = sortedResources.head
        val updatedCandidate = candidate.copy(currentWeight = math.min(weightManipulator(candidate.currentWeight), candidate.maxWeight))

        val alteredList = updatedCandidate +: sortedResources.tail

        (candidate.asInstanceOf[A], alteredList.asInstanceOf[F[A]])
    }
  }
}