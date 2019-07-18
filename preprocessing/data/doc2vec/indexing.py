
import nmslib
import numpy

# create a random matrix to index
data_matrix = numpy.loadtxt('docvectors.txt')

# initialize a new index, using a HNSW index on Cosine Similarity
index = nmslib.init(method='hnsw', space='l2')
index.addDataPointBatch(data_matrix)
index.createIndex({'post': 2}, print_progress=True)

# query for the nearest neighbours of the first datapoint
ids, distances = index.knnQuery(data_matrix[97], k=100)



for id, distance in zip(ids, distances):
    print(id, distance)

# get all nearest neighbours for all the datapoint
# using a pool of 4 threads to compute
#neighbours = index.knnQueryBatch(data, k=10, num_threads=4)


