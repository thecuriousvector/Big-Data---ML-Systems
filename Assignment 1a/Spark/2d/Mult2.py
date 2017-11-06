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
matrix = matrixRDD.map(lambda x : gen_matrix(x))

vectorRDD = sc.textFile("/user/rdv253/VectorInput.txt")
total_lines = vectorRDD.count()

no_of_splits = 3
rows_per_part = total_lines/no_of_splits
for count in range(1, no_of_splits+1):
     smallFile = sc.emptyRDD()
     for i in range(1, rows_per_part+1):
	     row = vectorRDD.first()
             smallFile = sc.union([smallFile, sc.parallelize(vectorRDD.take(1))])
	     vec = vectorRDD.collect()
	     vec = vec[1:]
	     vectorRDD = sc.parallelize(vec)
     path = '/user/rdv253/Vectors/'+str(count)+''
     smallFile.saveAsTextFile(path)

matrix = matrixRDD.map(lambda x : gen_matrix(x))

result = sc.emptyRDD()

for i in range (1, no_of_splits):
	path = '/user/rdv253/Vectors/'+str(i)+'/part-00001'
	vectorData = sc.textFile(path)
	vector = vectorData.map(lambda x : gen_vector(x))
	combined = matrix.join(vector)
	newCombined = combined.map(lambda x: (x[0],)+x[1])
	product = newCombined.map(lambda (x,y,z) : (y[0],y[1]*z))
	result += product

result=result.reduceByKey(lambda x,y: x+y)
result.coalesce(1).saveAsTextFile("Mult2_Output.txt")
