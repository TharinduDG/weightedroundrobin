package weightedroundrobin

import java.util.concurrent.TimeUnit

import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import RoundRobinUtils._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global


class RoundRobinForResourceAsyncPropertyTests extends PropSpec with PropertyChecks with Matchers {

  val expectedException = ResourceConsumer.resourceConsumptionError

  val rr = new RoundRobin[WeightedResource, Seq] {}

  val roundRobinAsyncTrigger: ((WeightedResource) => Future[Any], Seq[WeightedResource]) => ((Seq[WeightedResource]) => Seq[WeightedResource]) => (Future[Seq[WeightedResource]], Future[Either[Throwable, Any]])
  = rr.forResourceAsync(_)(plusOneRewarder, divByFivePenalizer, _)

  property("`forResourceAsync` `successCounter` should be incremented for each successful consumption of resource") {
    val resourcePool = mutable.ListBuffer(WeightedResource("nr1", "nr1", 100, 75), WeightedResource("nr2", "nr2", 40, 39), WeightedResource("nr3", "nr3", 50, 0))

    val resourceConsumer = new ResourceConsumer

    val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(resourcePool)

    val successfulConsumptionScenario =
      Table(
        "fn",
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _)

    var finalResultF: (Future[Seq[WeightedResource]], Future[Either[Throwable, Any]]) = (Future.successful(resourcePool), Future.successful(Left(new RuntimeException("No result!"))))

    val initialResourcePool = resourcePool.clone()

    forAll(successfulConsumptionScenario) { fn =>
      val pool = roundRobinAsyncTrigger(fn, resourcePool)(syncOriginalWithUpdated)
      finalResultF = pool

      Await.result(finalResultF._1, new FiniteDuration(5, TimeUnit.SECONDS))
      Await.result(finalResultF._2, new FiniteDuration(5, TimeUnit.SECONDS))
    }

    val finalPool = Await.result(finalResultF._1, new FiniteDuration(5, TimeUnit.SECONDS))
    val result = Await.result(finalResultF._2, new FiniteDuration(5, TimeUnit.SECONDS))

    resourceConsumer.successCounter.get() should be(successfulConsumptionScenario.toList.size)
    resourceConsumer.failureCounter.get() should be(0)
    result.right.get.asInstanceOf[Int] should be(resourceConsumer.successCounter.get())
    finalPool.maxBy(_.currentWeight).currentWeight should be(initialResourcePool.maxBy(_.currentWeight).currentWeight + successfulConsumptionScenario.toList.size)
    finalPool.minBy(_.currentWeight).currentWeight should be(0)
  }


  property("`forResourceAsync` candidate `WeightedResource` should be changed dynamically") {
    val resourcePool = mutable.ListBuffer(WeightedResource("nr1", "nr1", 100, 36), WeightedResource("nr2", "nr2", 40, 38), WeightedResource("nr3", "nr3", 50, 40))

    val resourceConsumer = new ResourceConsumer

    val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(resourcePool)

    val mixedScenario =
      Table(
        "fn",
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _,
        resourceConsumer.consumeResourceSuccessfullyAsync _)

    var finalResultF: (Future[Seq[WeightedResource]], Future[Either[Throwable, Any]]) = (Future.successful(resourcePool), Future.successful(Left(new RuntimeException("No result!"))))

    val initialResourcePool = resourcePool.clone()

    forAll(mixedScenario) { fn =>
      val pool = roundRobinAsyncTrigger(fn, resourcePool)(syncOriginalWithUpdated)
      finalResultF = pool

      Await.result(finalResultF._1, new FiniteDuration(5, TimeUnit.SECONDS))
      Await.result(finalResultF._2, new FiniteDuration(5, TimeUnit.SECONDS))
    }

    val finalPool = Await.result(finalResultF._1, new FiniteDuration(5, TimeUnit.SECONDS))
    val result = Await.result(finalResultF._2, new FiniteDuration(5, TimeUnit.SECONDS))

    resourceConsumer.successCounter.get() should be(6)
    resourceConsumer.failureCounter.get() should be(2)
    result.right.get.asInstanceOf[Int] should be(resourceConsumer.successCounter.get())
    finalPool.find(_.key == "nr3").get.currentWeight should be((initialResourcePool.find(_.key == "nr3").get.currentWeight + 2) / 5)
    finalPool.find(_.key == "nr2").get.currentWeight should be((initialResourcePool.find(_.key == "nr2").get.currentWeight + 2) / 5)
    finalPool.find(_.key == "nr1").get.currentWeight should be(initialResourcePool.find(_.key == "nr1").get.currentWeight + 2)
  }

  property("`forResourceAsync` when all failures, `WeightedResource` should be penalized to zero") {
    val resourcePool = mutable.ListBuffer(WeightedResource("nr1", "nr1", 100, 36), WeightedResource("nr2", "nr2", 40, 38), WeightedResource("nr3", "nr3", 50, 40))

    val resourceConsumer = new ResourceConsumer

    val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(resourcePool)

    val totalFailureScenario =
      Table(
        "fn",
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceFailureAsync _,
        resourceConsumer.consumeResourceFailureAsync _)

    var finalResultF: (Future[Seq[WeightedResource]], Future[Either[Throwable, Any]]) = (Future.successful(resourcePool), Future.successful(Left(new RuntimeException("No result!"))))

    forAll(totalFailureScenario) { fn =>
      val pool = roundRobinAsyncTrigger(fn, resourcePool)(syncOriginalWithUpdated)
      finalResultF = pool

      Await.result(finalResultF._1, new FiniteDuration(5, TimeUnit.SECONDS))
      Await.result(finalResultF._2, new FiniteDuration(5, TimeUnit.SECONDS))
    }

    val finalPool = Await.result(finalResultF._1, new FiniteDuration(5, TimeUnit.SECONDS))
    val result = Await.result(finalResultF._2, new FiniteDuration(5, TimeUnit.SECONDS))

    resourceConsumer.successCounter.get() should be(0)
    resourceConsumer.failureCounter.get() should be(totalFailureScenario.toList.size)
    result.left.get should be(expectedException)

    finalPool.find(_.key == "nr3").get.currentWeight should be(0)
    finalPool.find(_.key == "nr2").get.currentWeight should be(0)
    finalPool.find(_.key == "nr1").get.currentWeight should be(0)
  }
}