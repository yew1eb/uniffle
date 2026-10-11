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
  spark.range(0, 2000000, 1, 20)
       .mapPartitions { it => {
         val ctx = TaskContext.get()
         FaultyIterator(
           it,
           ctx.partitionId == 2 && ctx.attemptNumber < 3,
           Some(ctx.partitionId == 4).filter(v => v).map(_ => 250000)
         )
       }}
       .groupBy(($"value" / 200000).cast("int"))
       .as[Long, Long]
       .mapGroups{(id, it) => (id, it.length)}
       .sort("_1")
       .collect
  )

assert(result.sameElements(Array((0,200000), (1,200000), (2,200000), (3,200000), (4,200000), (5,200000), (6,200000), (7,200000), (8,200000), (9,200000))))
println("DEMO_RESULT_OK: " + result.mkString(","))
