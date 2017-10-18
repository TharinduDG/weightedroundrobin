package weightedroundrobin

import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import RoundRobinUtils._

class RoundRobinForResourcePropertyTests extends PropSpec with PropertyChecks with Matchers {

  val expectedException = ResourceConsumer.resourceConsumptionError

  val rr = new RoundRobin[WeightedResource, Seq] {}

  val roundRobinTrigger: ((WeightedResource) => Any, Seq[WeightedResource]) => ((Seq[WeightedResource]) => Seq[WeightedResource]) => (Seq[WeightedResource], Either[Throwable, Any]) =
    rr.forResource(_)(plusOneRewarder, divByFivePenalizer, _)

  property("`forResource` `successCounter` should be incremented for each successful consumption of resource") {
    val resourcePool = mutable.ListBuffer(WeightedResource("nr1", "nr1", 100, 75), WeightedResource("nr2", "nr2", 40, 39), WeightedResource("nr3", "nr3", 50, 0))

    val resourceConsumer = new ResourceConsumer

    val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(resourcePool)

    val successfulConsumptionScenario =
      Table(
        "fn",
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _)

    var finalResult: (Seq[WeightedResource], Either[Throwable, Any]) = (resourcePool, Left(new RuntimeException("No result!")))

    val initialResourcePool = resourcePool.clone()

    forAll(successfulConsumptionScenario) { fn =>
      val pool = roundRobinTrigger(fn, resourcePool)(syncOriginalWithUpdated)
      finalResult = pool
      pool._2.isRight should be(true)
    }

    resourceConsumer.successCounter.get() should be(successfulConsumptionScenario.toList.size)
    resourceConsumer.failureCounter.get() should be(0)
    finalResult._2.right.get.asInstanceOf[Int] should be(resourceConsumer.successCounter.get())
    finalResult._1.maxBy(_.currentWeight).currentWeight should be(initialResourcePool.maxBy(_.currentWeight).currentWeight + successfulConsumptionScenario.toList.size)
    finalResult._1.minBy(_.currentWeight).currentWeight should be(0)
  }


  property("`forResource` candidate `WeightedResource` should be changed dynamically") {
    val resourcePool = mutable.ListBuffer(WeightedResource("nr1", "nr1", 100, 36), WeightedResource("nr2", "nr2", 40, 38), WeightedResource("nr3", "nr3", 50, 40))

    val resourceConsumer = new ResourceConsumer

    val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(resourcePool)

    val mixedScenario =
      Table(
        "fn",
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceSuccessfully _,
        resourceConsumer.consumeResourceSuccessfully _)

    var finalResult: (Seq[WeightedResource], Either[Throwable, Any]) = (resourcePool, Left(new RuntimeException("No result!")))

    val initialResourcePool = resourcePool.clone()

    forAll(mixedScenario) { fn =>
      val pool = roundRobinTrigger(fn, resourcePool)(syncOriginalWithUpdated)
      finalResult = pool
    }

    resourceConsumer.successCounter.get() should be(6)
    resourceConsumer.failureCounter.get() should be(2)
    finalResult._2.right.get.asInstanceOf[Int] should be(resourceConsumer.successCounter.get())
    finalResult._1.find(_.key == "nr3").get.currentWeight should be((initialResourcePool.find(_.key == "nr3").get.currentWeight + 2) / 5)
    finalResult._1.find(_.key == "nr2").get.currentWeight should be((initialResourcePool.find(_.key == "nr2").get.currentWeight + 2) / 5)
    finalResult._1.find(_.key == "nr1").get.currentWeight should be(initialResourcePool.find(_.key == "nr1").get.currentWeight + 2)
  }

  property("`forResource` when all failures, `WeightedResource` should be penalized to zero") {
    val resourcePool = mutable.ListBuffer(WeightedResource("nr1", "nr1", 100, 36), WeightedResource("nr2", "nr2", 40, 38), WeightedResource("nr3", "nr3", 50, 40))

    val resourceConsumer = new ResourceConsumer

    val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(resourcePool)

    val totalFailureScenario =
      Table(
        "fn",
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceFailure _,
        resourceConsumer.consumeResourceFailure _)

    var finalResult: (Seq[WeightedResource], Either[Throwable, Any]) = (resourcePool, Left(new RuntimeException("No result!")))

    forAll(totalFailureScenario) { fn =>
      val pool = roundRobinTrigger(fn, resourcePool)(syncOriginalWithUpdated)
      finalResult = pool
      pool._2.isLeft should be(true)
    }

    resourceConsumer.successCounter.get() should be(0)
    resourceConsumer.failureCounter.get() should be(totalFailureScenario.toList.size)
    finalResult._2.left.get should be(expectedException)
    finalResult._1.find(_.key == "nr3").get.currentWeight should be(0)
    finalResult._1.find(_.key == "nr2").get.currentWeight should be(0)
    finalResult._1.find(_.key == "nr1").get.currentWeight should be(0)
  }
}