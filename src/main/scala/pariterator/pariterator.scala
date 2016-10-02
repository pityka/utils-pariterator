package pariterator

import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}

class CustomNamedJoinWorkerThread(_pool: ForkJoinPool)
    extends ForkJoinWorkerThread(_pool)
