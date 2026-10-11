import org.apache.spark.TaskContext

// fails iteration (at the end) or delays iteration (each element)
// failing tasks negates iterator values, shuffle data of failing task must not leak into next stage
case class FaultyIterator(it: Iterator[java.lang.Long], fail: Boolean, sleep: Option[Int]) extends Iterator[java.lang.Long] {
  override def hasNext: Boolean = it.hasNext || fail
  override def next(): java.lang.Long = {
    // delay iteration if requested
    if (sleep.isDefined) {
      val start = System.nanoTime()
      while (start + sleep.get >= System.nanoTime()) { }
    }

    // fail at the end if requested
    if (fail && !it.hasNext) throw new RuntimeException()

    // just iterate
    if (fail) {
      -it.next()
    } else {
      it.next()
    }
  }
}

// we fail task two 3 times and delay task four so we see a speculative execution
val result = (
  spark.range(0, 10000000, 1, 100)
       .mapPartitions { it => {
         val ctx = TaskContext.get()
         FaultyIterator(
           it,
           ctx.partitionId == 2 && ctx.attemptNumber < 3,
           Some(ctx.partitionId == 4).filter(v => v).map(_ => 250000)
         )
       }}
       .groupBy(($"value" / 1000000).cast("int"))
       .as[Long, Long]
       .mapGroups{(id, it) => (id, it.length)}
       .sort("_1")
       .collect
  )

assert(result.sameElements(Array((0,1000000), (1,1000000), (2,1000000), (3,1000000), (4,1000000), (5,1000000), (6,1000000), (7,1000000), (8,1000000), (9,1000000))))
println("DEMO_RESULT_OK: " + result.mkString(","))
