package weightedroundrobin

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by tgalappathth on 10/4/17.
  */
class ResourceConsumer {

  val successCounter = new AtomicInteger()
  val failureCounter = new AtomicInteger()

  def consumeResourceSuccessfully(r: Resource): Int = {
    successCounter.incrementAndGet()
  }

  def consumeResourceFailure(r: Resource): Int = {
    failureCounter.incrementAndGet()
    throw ResourceConsumer.resourceConsumptionError
  }

  def consumeResourceSuccessfullyAsync(r: Resource): Future[Int] = {
    Future {
      successCounter.incrementAndGet()
    }
  }

  def consumeResourceFailureAsync(r: Resource): Future[Int] = {
    Future {
      failureCounter.incrementAndGet()
      throw ResourceConsumer.resourceConsumptionError
    }
  }
}

case class ResourceConsumptionException(message: String) extends RuntimeException(message)

object ResourceConsumer {
  val resourceConsumptionError = ResourceConsumptionException("couldn't consume resource")
}
