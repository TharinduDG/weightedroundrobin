package weightedroundrobin

import java.util.concurrent.TimeUnit

import org.specs2.mutable.Specification

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

/**
  * Created by tgalappathth on 9/29/17.
  */
class RoundRobinSpec extends Specification {

  sequential

  import RoundRobinUtils._

  val nr1 = NeutralResource("NR1", "1")
  val nr2 = NeutralResource("NR2", "2")
  val nr3 = NeutralResource("NR3", "3")
  val nr4 = NeutralResource("NR4", "4")

  // unused weight resources
  val wr1 = WeightedResource("WR1", "1", 11, 11)
  val wr2 = WeightedResource("WR2", "2", 21, 21)
  val wr3 = WeightedResource("WR3", "3", 100, 100)
  val wr4 = WeightedResource("WR4", "4", 51, 51)

  // consumed weight resources
  val uwr1 = WeightedResource("UWR1", "1", 11, 9)
  val uwr2 = WeightedResource("UWR2", "2", 21, 8)
  val uwr3 = WeightedResource("UWR3", "3", 100, 25)
  val uwr4 = WeightedResource("UWR4", "4", 51, 30)

  val expectedException = new RuntimeException("error while consuming resource")

  "RoundRobin" should {
    "rotate `NeutralResource` list when `forResource` with non failing `fn`" in {
      val neutralResourcePool: ListBuffer[NeutralResource] = ListBuffer[NeutralResource](nr1, nr2, nr3, nr4)

      val rr = new RoundRobin[NeutralResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[NeutralResource]) => ListBuffer[NeutralResource] = syncGlobalWithUpdatedResourcePool(neutralResourcePool)
      val (currentResourcePool, result) = rr.forResource(consumeNeutralResource)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)

      val expected = ListBuffer(nr2, nr3, nr4, nr1)
      result.isRight must_== true
      result.right.get must_== nr1.value.toInt
      currentResourcePool must_== expected
    }

    "rotate `NeutralResource` list when `forResource` is  run for three times with non failing `fn`" in {
      val neutralResourcePool: ListBuffer[NeutralResource] = ListBuffer[NeutralResource](nr1, nr2, nr3, nr4)

      val rr = new RoundRobin[NeutralResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[NeutralResource]) => ListBuffer[NeutralResource] = syncGlobalWithUpdatedResourcePool(neutralResourcePool)

      rr.forResource(consumeNeutralResource)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      rr.forResource(consumeNeutralResource)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      val (currentResourcePool, result) = rr.forResource(consumeNeutralResource)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)

      val expected = ListBuffer(nr4, nr1, nr2, nr3)
      result.isRight must_== true
      result.right.get must_== nr3.value.toInt
      currentResourcePool must_== expected
    }

    "rotate `NeutralResource` list when `forResource` with failing `fn`" in {
      val neutralResourcePool: ListBuffer[NeutralResource] = ListBuffer[NeutralResource](nr1, nr2, nr3, nr4)

      val rr = new RoundRobin[NeutralResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[NeutralResource]) => ListBuffer[NeutralResource] = syncGlobalWithUpdatedResourcePool(neutralResourcePool)

      val (currentResourcePool, result) = rr.forResource(consumeNeutralResourceWithError)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)

      val expected = ListBuffer(nr2, nr3, nr4, nr1)
      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool must_== expected
    }

    "rotate `NeutralResource` list when `forResource` is  run for three times with failing `fn`" in {
      val neutralResourcePool: ListBuffer[NeutralResource] = ListBuffer[NeutralResource](nr1, nr2, nr3, nr4)

      val rr = new RoundRobin[NeutralResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[NeutralResource]) => ListBuffer[NeutralResource] = syncGlobalWithUpdatedResourcePool(neutralResourcePool)

      rr.forResource(consumeNeutralResourceWithError)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      rr.forResource(consumeNeutralResourceWithError)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      val (currentResourcePool, result) = rr.forResource(consumeNeutralResourceWithError)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)

      val expected = ListBuffer(nr4, nr1, nr2, nr3)
      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool must_== expected
    }

    "rotate `NeutralResource` list when `forResourceAsync` with non failing `fn`" in {
      val neutralResourcePool: ListBuffer[NeutralResource] = ListBuffer[NeutralResource](nr1, nr2, nr3, nr4)

      val rr = new RoundRobin[NeutralResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[NeutralResource]) => ListBuffer[NeutralResource] = syncGlobalWithUpdatedResourcePool(neutralResourcePool)

      val response = rr.forResourceAsync(consumeNeutralResourceAsync)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      val expected = ListBuffer(nr2, nr3, nr4, nr1)

      result.isRight must_== true
      result.right.get must_== nr1.value.toInt
      currentResourcePool must_== expected
    }


    "rotate `NeutralResource` list when `forResourceAsync` is  run for three times with non failing `fn`" in {
      val neutralResourcePool: ListBuffer[NeutralResource] = ListBuffer[NeutralResource](nr1, nr2, nr3, nr4)

      val rr = new RoundRobin[NeutralResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[NeutralResource]) => ListBuffer[NeutralResource] = syncGlobalWithUpdatedResourcePool(neutralResourcePool)

      rr.forResourceAsync(consumeNeutralResourceAsync)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      rr.forResourceAsync(consumeNeutralResourceAsync)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      val response = rr.forResourceAsync(consumeNeutralResourceAsync)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      val expected = ListBuffer(nr4, nr1, nr2, nr3)

      result.isRight must_== true
      result.right.get must_== nr3.value.toInt
      currentResourcePool must_== expected
    }

    "rotate `NeutralResource` list when `forResourceAsync` with failing `fn`" in {
      val neutralResourcePool: ListBuffer[NeutralResource] = ListBuffer[NeutralResource](nr1, nr2, nr3, nr4)

      val rr = new RoundRobin[NeutralResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[NeutralResource]) => ListBuffer[NeutralResource] = syncGlobalWithUpdatedResourcePool(neutralResourcePool)

      val response = rr.forResourceAsync(consumeNeutralResourceAsyncWithError)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      val expected = ListBuffer(nr2, nr3, nr4, nr1)

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool must_== expected
    }


    "rotate `NeutralResource` list when `forResourceAsync` is  run for three times with failing `fn`" in {
      val neutralResourcePool: ListBuffer[NeutralResource] = ListBuffer[NeutralResource](nr1, nr2, nr3, nr4)

      val rr = new RoundRobin[NeutralResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[NeutralResource]) => ListBuffer[NeutralResource] = syncGlobalWithUpdatedResourcePool(neutralResourcePool)

      rr.forResourceAsync(consumeNeutralResourceAsyncWithError)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      rr.forResourceAsync(consumeNeutralResourceAsyncWithError)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      val response = rr.forResourceAsync(consumeNeutralResourceAsyncWithError)(plusOneRewarder, divByFivePenalizer, neutralResourcePool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      val expected = ListBuffer(nr4, nr1, nr2, nr3)

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool must_== expected
    }

    // weight resources with full weight
    "rotate `WeightedResource` list when `forResource` with non failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](wr1, wr2, wr3, wr4)

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      val (currentResourcePool, result) = rr.forResource(consumeWeightedResource)(plusOneRewarder, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)

      result.isRight must_== true
      result.right.get must_== List(wr1, wr2, wr3, wr4).maxBy(_.currentWeight).value.toInt
      currentResourcePool.sortBy(_.currentWeight) must_== ListBuffer(wr1, wr2, wr3, wr4).sortBy(_.currentWeight)
    }

    "rotate `WeightedResource` list when `forResource` is  run for three times with non failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](wr1, wr2, wr3, wr4)

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      rr.forResource(consumeWeightedResource)(plusOneRewarder, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)
      rr.forResource(consumeWeightedResource)(plusOneRewarder, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)
      val (currentResourcePool, result) = rr.forResource(consumeWeightedResource)(plusOneRewarder, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)

      result.isRight must_== true
      result.right.get must_== List(wr1, wr2, wr3, wr4).maxBy(_.currentWeight).value.toInt
      currentResourcePool.sortBy(_.currentWeight) must_== ListBuffer(wr1, wr2, wr3, wr4).sortBy(_.currentWeight)
    }

    "rotate `WeightedResource` list when `forResource` with failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](wr1, wr2, wr3, wr4)

      val expectedMaxWeightedResource = weightedResourcesPool.maxBy(_.currentWeight)

      val penalizerFn = divByFivePenalizer _

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      val (currentResourcePool, result) = rr.forResource(consumeWeightedResourceWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool.find(_.key == expectedMaxWeightedResource.key).get.currentWeight must_== penalizerFn(expectedMaxWeightedResource.maxWeight)
    }

    "rotate `WeightedResource` list when `forResource` is  run for three times with failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](wr1, wr2, wr3, wr4)

      val penalizerFn = divByFivePenalizer _

      val expectedWeightResourcesPool = weightedResourcesPool.clone()
      (1 to 3).foreach { _ =>
        val mxwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
        val updatedWR = mxwr.copy(currentWeight = penalizerFn(mxwr.currentWeight))
        expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxwr), updatedWR)
      }

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      rr.forResource(consumeWeightedResourceWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      rr.forResource(consumeWeightedResourceWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val (currentResourcePool, result) = rr.forResource(consumeWeightedResourceWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

    "rotate `WeightedResource` list when `forResourceAsync` with non failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](wr1, wr2, wr3, wr4)

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      val response = rr.forResourceAsync(consumeWeightedResourceAsync)(plusOneRewarder, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      result.isRight must_== true
      result.right.get must_== List(wr1, wr2, wr3, wr4).maxBy(_.currentWeight).value.toInt
      currentResourcePool.find(_.key == wr3.key).get.currentWeight must_== wr3.maxWeight
    }

    "rotate `WeightedResource` list when `forResourceAsync` is  run for three times with non failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](wr1, wr2, wr3, wr4)

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val penalizerFn = divByFivePenalizer _

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      rr.forResourceAsync(consumeWeightedResourceAsync)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      rr.forResourceAsync(consumeWeightedResourceAsync)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val response = rr.forResourceAsync(consumeWeightedResourceAsync)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      result.isRight must_== true
      result.right.get must_== List(wr1, wr2, wr3, wr4).maxBy(_.currentWeight).value.toInt
      currentResourcePool.sortBy(_.currentWeight) must_== ListBuffer(wr1, wr2, wr3, wr4).sortBy(_.currentWeight)
    }

    "rotate `WeightedResource` list when `forResourceAsync` with failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](wr1, wr2, wr3, wr4)

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val penalizerFn = divByFivePenalizer _

      val expectedWeightResourcesPool = weightedResourcesPool.clone()

      val mxwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
      val updatedWR = mxwr.copy(currentWeight = penalizerFn(mxwr.currentWeight))
      expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxwr), updatedWR)

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      val response = rr.forResourceAsync(consumeWeightedResourceAsyncWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

    "rotate `WeightedResource` list when `forResourceAsync` is  run for three times with failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](wr1, wr2, wr3, wr4)

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val penalizerFn = divByFivePenalizer _

      val expectedWeightResourcesPool = weightedResourcesPool.clone()
      (1 to 3).foreach { _ =>
        val mxwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
        val updatedWR = mxwr.copy(currentWeight = penalizerFn(mxwr.currentWeight))
        expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxwr), updatedWR)
      }

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      rr.forResourceAsync(consumeWeightedResourceAsyncWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      rr.forResourceAsync(consumeWeightedResourceAsyncWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val response = rr.forResourceAsync(consumeWeightedResourceAsyncWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

    //weight resources with reduced weights
    "rotate consumed `WeightedResource` list when `forResource` with non failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](uwr1, uwr2, uwr3, uwr4)
      val expectedWeightResourcesPool = weightedResourcesPool.clone()

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      val (currentResourcePool, result) = rr.forResource(consumeWeightedResource)(plusOneRewarder, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)

      val rewarderFn = plusOneRewarder _

      val mxwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
      val updatedWR = mxwr.copy(currentWeight = rewarderFn(mxwr.currentWeight))
      expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxwr), updatedWR)

      result.isRight must_== true
      result.right.get must_== List(uwr1, uwr2, uwr3, uwr4).maxBy(_.currentWeight).value.toInt
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

    "rotate consumed `WeightedResource` list when `forResource` is  run for three times with non failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](uwr1, uwr2, uwr3, uwr4)
      val expectedWeightResourcesPool = weightedResourcesPool.clone()

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      rr.forResource(consumeWeightedResource)(plusOneRewarder, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)
      rr.forResource(consumeWeightedResource)(plusOneRewarder, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)
      val (currentResourcePool, result) = rr.forResource(consumeWeightedResource)(plusOneRewarder, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)

      val rewarderFn = plusOneRewarder _

      (1 to 3).foreach { _ =>
        val mxwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
        val updatedWR = mxwr.copy(currentWeight = rewarderFn(mxwr.currentWeight))
        expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxwr), updatedWR)
      }

      result.isRight must_== true
      result.right.get must_== List(uwr1, uwr2, uwr3, uwr4).maxBy(_.currentWeight).value.toInt
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

    "rotate consumed `WeightedResource` list when `forResource` with failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](uwr1, uwr2, uwr3, uwr4)

      val expectedMaxWeightedResource = weightedResourcesPool.maxBy(_.currentWeight)

      val penalizerFn = divByFivePenalizer _

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      val (currentResourcePool, result) = rr.forResource(consumeWeightedResourceWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool.find(_.key == expectedMaxWeightedResource.key).get.currentWeight must_== penalizerFn(expectedMaxWeightedResource.currentWeight)
    }

    "rotate consumed `WeightedResource` list when `forResource` is  run for three times with failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](uwr1, uwr2, uwr3, uwr4)

      val penalizerFn = divByFivePenalizer _

      val expectedWeightResourcesPool = weightedResourcesPool.clone()
      (1 to 3).foreach { _ =>
        val mxuwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
        val updateduwr = mxuwr.copy(currentWeight = penalizerFn(mxuwr.currentWeight))
        expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxuwr), updateduwr)
      }

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      rr.forResource(consumeWeightedResourceWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      rr.forResource(consumeWeightedResourceWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val (currentResourcePool, result) = rr.forResource(consumeWeightedResourceWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

    "rotate consumed `WeightedResource` list when `forResourceAsync` with non failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](uwr1, uwr2, uwr3, uwr4)
      val expectedWeightResourcesPool = weightedResourcesPool.clone()

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val rewarderFn = plusOneRewarder _

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      val response = rr.forResourceAsync(consumeWeightedResourceAsync)(rewarderFn, divByFivePenalizer, weightedResourcesPool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      val mxwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
      val updatedWR = mxwr.copy(currentWeight = rewarderFn(mxwr.currentWeight))
      expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxwr), updatedWR)

      result.isRight must_== true
      result.right.get must_== List(uwr1, uwr2, uwr3, uwr4).maxBy(_.currentWeight).value.toInt
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

    "rotate consumed `WeightedResource` list when `forResourceAsync` is  run for three times with non failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](uwr1, uwr2, uwr3, uwr4)

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val rewarderFn = plusOneRewarder _

      val expectedWeightResourcesPool = weightedResourcesPool.clone()
      (1 to 3).foreach { _ =>
        val mxwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
        val updatedWR = mxwr.copy(currentWeight = rewarderFn(mxwr.currentWeight))
        expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxwr), updatedWR)
      }

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      rr.forResourceAsync(consumeWeightedResourceAsync)(plusOneRewarder, rewarderFn, weightedResourcesPool)(syncOriginalWithUpdated)
      rr.forResourceAsync(consumeWeightedResourceAsync)(plusOneRewarder, rewarderFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val response = rr.forResourceAsync(consumeWeightedResourceAsync)(plusOneRewarder, rewarderFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      result.isRight must_== true
      result.right.get must_== List(uwr1, uwr2, uwr3, uwr4).maxBy(_.currentWeight).value.toInt
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

    "rotate consumed `WeightedResource` list when `forResourceAsync` with failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](uwr1, uwr2, uwr3, uwr4)

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val penalizerFn = divByFivePenalizer _

      val expectedWeightResourcesPool = weightedResourcesPool.clone()

      val mxuwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
      val updateduwr = mxuwr.copy(currentWeight = penalizerFn(mxuwr.currentWeight))
      expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxuwr), updateduwr)

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      val response = rr.forResourceAsync(consumeWeightedResourceAsyncWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

    "rotate consumed `WeightedResource` list when `forResourceAsync` is  run for three times with failing `fn`" in {
      val weightedResourcesPool: ListBuffer[WeightedResource] = ListBuffer[WeightedResource](uwr1, uwr2, uwr3, uwr4)

      val rr = new RoundRobin[WeightedResource, Seq] {}

      val penalizerFn = divByFivePenalizer _

      val expectedWeightResourcesPool = weightedResourcesPool.clone()
      (1 to 3).foreach { _ =>
        val mxuwr = expectedWeightResourcesPool.maxBy(_.currentWeight)
        val updateduwr = mxuwr.copy(currentWeight = penalizerFn(mxuwr.currentWeight))
        expectedWeightResourcesPool.update(expectedWeightResourcesPool.indexOf(mxuwr), updateduwr)
      }

      val syncOriginalWithUpdated: (Seq[WeightedResource]) => ListBuffer[WeightedResource] = syncGlobalWithUpdatedResourcePool(weightedResourcesPool)

      rr.forResourceAsync(consumeWeightedResourceAsyncWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      rr.forResourceAsync(consumeWeightedResourceAsyncWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val response = rr.forResourceAsync(consumeWeightedResourceAsyncWithError)(plusOneRewarder, penalizerFn, weightedResourcesPool)(syncOriginalWithUpdated)
      val currentResourcePool = Await.result(response._1, new FiniteDuration(5, TimeUnit.SECONDS))
      val result = Await.result(response._2, new FiniteDuration(5, TimeUnit.SECONDS))

      result.isLeft must_== true
      result.left.get must_== expectedException
      currentResourcePool.sortBy(_.currentWeight) must_== expectedWeightResourcesPool.sortBy(_.currentWeight)
    }

  }

  def consumeNeutralResource(r: NeutralResource): Int = {
    r.value.toInt
  }

  def consumeNeutralResourceWithError(r: NeutralResource): Int = {
    throw expectedException
  }

  def consumeNeutralResourceAsync(r: NeutralResource): Future[Int] = {
    Future {
      r.value.toInt
    }
  }

  def consumeNeutralResourceAsyncWithError(r: NeutralResource): Future[Int] = {
    Future[Int] {
      throw expectedException
    }
  }

  def consumeWeightedResource(r: WeightedResource): Int = {
    r.value.toInt
  }

  def consumeWeightedResourceWithError(r: WeightedResource): Int = {
    throw expectedException
  }

  def consumeWeightedResourceAsync(r: WeightedResource): Future[Int] = {
    Future {
      r.value.toInt
    }
  }

  def consumeWeightedResourceAsyncWithError(r: WeightedResource): Future[Int] = {
    Future[Int] {
      throw expectedException
    }
  }
}
