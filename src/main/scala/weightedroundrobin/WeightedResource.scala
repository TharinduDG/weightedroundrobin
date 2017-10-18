package weightedroundrobin

/**
  * Created by tgalappathth on 9/29/17.
  */


sealed trait Resource {
  val key: String
  val value: String
}

sealed case class NeutralResource(override val key: String, override val value: String) extends Resource

sealed case class WeightedResource(override val key: String, override val value: String, maxWeight: Int, currentWeight: Int) extends Resource {
  require(maxWeight >= 0, "maxWeight should be non negative!")
  require(currentWeight >= 0, "currentWeight should be non negative!")
  require(maxWeight >= currentWeight, "maxWeight should not be less than currentWeight")
}