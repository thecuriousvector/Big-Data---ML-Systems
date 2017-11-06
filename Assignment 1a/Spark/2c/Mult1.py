from pyspark import SparkContext, SparkConf
sc = SparkContext()

def gen_vector(line):
	inputs = line.split(',')
	col = int(inputs[0])
	value = int(inputs[1])
	return (col, value)


def gen_matrix(line):
        inputs = line.split(',')
        row = int(inputs[0])
        col = int(inputs[1])
	value = int(inputs[2])
        return (col, (row,value))


matrixRDD = sc.textFile("/user/rdv253/MatrixInput.txt")
vectorRDD = sc.textFile("/user/rdv253/VectorInput.txt")

vector = vectorRDD.map(lambda x : gen_vector(x))
matrix = matrixRDD.map(lambda x : gen_matrix(x))

combined = matrix.join(vector)
newCombined = combined.map(lambda x: (x[0],)+x[1])

product = newCombined.map(lambda (x,y,z) : (y[0],y[1]*z))
result = product.reduceByKey(lambda x,y: x+y)
result.saveAsTextFile("Mult1_Output.txt");
