def distance(p:Vector[Double], q:Vector[Double]) : Double = {
    math.sqrt(p.zip(q).map(pair => math.pow((pair._1 - pair._2),2)).reduce(_+_));
}
 
def clostestpoint(q: Vector[Double], candidates: Array[Vector[Double]]): Vector[Double] = {
    candidates.reduceLeft((a, b) => if (distance(q, a) < distance(q, b)) a else b)
}

def add_vec(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
    Vector(v1(0) + v2(0), v1(1) + v2(1));
}
 
def average(cluster: Iterable[Vector[Double]]): Vector[Double] = {
    def avg(p: Vector[Double], n: Int) = Vector(p(0)/n, p(1)/n);
    avg(cluster.reduce(add_vec), cluster.size);
}

/******initialized data*******/
var lines = sc.textFile("./clustering_dataset.txt");
var data = lines.map(l => Vector.empty ++ l.split('\t').map(_.toDouble))
var k = 3
val array = data.collect

/******init the centroids*******/
val r = scala.util.Random
var centroids = new Array[Vector[Double]](3)
for(i <- 0 until 3){
    centroids(i) = array(r.nextInt(array.size))
}

/******kmeans*******/
var movement = Array(Double.PositiveInfinity,Double.PositiveInfinity,Double.PositiveInfinity)
var clusters = array.groupBy(clostestpoint(_,centroids))
while(movement.exists(_ > 0.0001)){
    var newCentroids = centroids.map(oldCentroid => {
                clusters.get(oldCentroid) match {
                    case Some(vector) => average(vector)
                    case None => oldCentroid
                }
            })
    movement = (centroids zip newCentroids).map({ case (a, b) => distance(a, b) })
    centroids = newCentroids
    clusters = array.groupBy(clostestpoint(_,centroids))
}

println("\n\nCentroids:" + "\n" + centroids.mkString("\n"))
//for ((k,v) <- clusters) printf("\nCentroids: %s\nvalue: %s\n", k, v.toSeq)
