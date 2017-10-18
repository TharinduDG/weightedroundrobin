package weightedroundrobin

import java.util.concurrent.TimeUnit

import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import weightedroundrobin.RoundRobinUtils._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}


class RoundRobinPropertyTestWithRandomGens extends PropSpec with PropertyChecks with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  val expectedException = ResourceConsumer.resourceConsumptionError

  val rr = new RoundRobin[WeightedResource, Seq] {}

  val resourceConsumer = new ResourceConsumer

  val weightedResourceGen: Arbitrary[WeightedResource] = Arbitrary[WeightedResource] {
    for {
      key <- arbitrary[String]
      value <- arbitrary[String]
      maxWeight <- Gen.oneOf[Int]((20 to 200).by(5))
      deviation <- Gen.oneOf[Int](0 to 20)
    } yield {
      WeightedResource(key, value, maxWeight, maxWeight - deviation)
    }
  }

  implicit val weightedResourcePoolGen = Arbitrary[mutable.ListBuffer[WeightedResource]] {
    for {
      weightedResourcesList <- Gen.nonEmptyListOf[WeightedResource](weightedResourceGen.arbitrary)
    } yield {
      val lb = mutable.ListBuffer[WeightedResource]()
      lb.appendAll(weightedResourcesList)
      lb
    }
  }

  implicit val consumerFnGen = Arbitrary[(Resource) => Int] {
    Gen.oneOf(List.fill[(Resource) => Int](10)(resourceConsumer.consumeResourceSuccessfully(_)) ++ List.fill(10)(resourceConsumer.consumeResourceFailure(_)))
  }

  implicit val consumerAsyncFnGen = Arbitrary[(Resource) => Future[Int]] {
    Gen.oneOf(List.fill[(Resource) => Future[Int]](10)(resourceConsumer.consumeResourceSuccessfullyAsync(_)) ++ List.fill(10)(resourceConsumer.consumeResourceFailureAsync(_)))
  }

  var previousSuccessCount = resourceConsumer.successCounter.get()
  var previousFailureCount = resourceConsumer.failureCounter.get()

  property("`forResourceUnsafe` all in one test") {

    val roundRobinTrigger: ((WeightedResource) => Any, Seq[WeightedResource]) => ((Seq[WeightedResource]) => Seq[WeightedResource]) => (Seq[WeightedResource], Either[Throwable, Any]) =
      rr.forResourceUnsafe(_)(plusOneRewarder, divByFivePenalizer, _)

    forAll { (resourcePool: mutable.ListBuffer[WeightedResource], fn: (Resource) => Int) =>

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(resourcePool)

      val consumableCandidate = resourcePool.maxBy(_.currentWeight)

      val (newPool, result) = roundRobinTrigger(fn, resourcePool)(syncOriginalWithUpdated)

      val consumedCandidate = newPool.find(_.key == consumableCandidate.key).get

      if (result.isRight) {

        if (consumableCandidate.currentWeight == consumableCandidate.maxWeight) consumableCandidate.currentWeight should be(consumedCandidate.currentWeight)
        else plusOneRewarder(consumableCandidate.currentWeight) should be(consumedCandidate.currentWeight)

        val count = result.right.get.asInstanceOf[Int]

        count should be(resourceConsumer.successCounter.get())
        count should be(previousSuccessCount + 1)
        previousSuccessCount = resourceConsumer.successCounter.get()
      } else {
        divByFivePenalizer(consumableCandidate.currentWeight) should be(consumedCandidate.currentWeight)
        resourceConsumer.failureCounter.get() should be(previousFailureCount + 1)
        previousFailureCount = resourceConsumer.failureCounter.get()
        result.left.get.isInstanceOf[ResourceConsumptionException] should be(true)
      }
    }
  }

  property("`forResourceAsyncUnsafe` all in one test") {

    val roundRobinAsyncTrigger: ((WeightedResource) => Future[Any], Seq[WeightedResource]) => ((Seq[WeightedResource]) => Seq[WeightedResource]) => (Future[Seq[WeightedResource]], Future[Either[Throwable, Any]])
    = rr.forResourceAsyncUnsafe(_)(plusOneRewarder, divByFivePenalizer, _)

    forAll { (resourcePool: mutable.ListBuffer[WeightedResource], fn: (Resource) => Future[Int]) =>

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(resourcePool)

      val consumableCandidate = resourcePool.maxBy(_.currentWeight)

      val (newPoolF, resultF) = roundRobinAsyncTrigger(fn, resourcePool)(syncOriginalWithUpdated)

      val newPool = Await.result(newPoolF, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(resultF, new FiniteDuration(5, TimeUnit.SECONDS))

      val consumedCandidate = newPool.find(_.key == consumableCandidate.key).get

      if (result.isRight) {

        if (consumableCandidate.currentWeight == consumableCandidate.maxWeight) consumableCandidate.currentWeight should be(consumedCandidate.currentWeight)
        else plusOneRewarder(consumableCandidate.currentWeight) should be(consumedCandidate.currentWeight)

        val count = result.right.get.asInstanceOf[Int]

        count should be(resourceConsumer.successCounter.get())
        count should be(previousSuccessCount + 1)
        previousSuccessCount = resourceConsumer.successCounter.get()
      } else {
        divByFivePenalizer(consumableCandidate.currentWeight) should be(consumedCandidate.currentWeight)
        resourceConsumer.failureCounter.get() should be(previousFailureCount + 1)
        previousFailureCount = resourceConsumer.failureCounter.get()
        result.left.get.isInstanceOf[ResourceConsumptionException] should be(true)
      }
    }
  }
}