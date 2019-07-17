import nmslib
import numpy

# create a random matrix to index
data_matrix = numpy.loadtxt('docvectors.txt')

# initialize a new index, using a HNSW index on Cosine Similarity
index = nmslib.init(method='hnsw', space='l2')
index.addDataPointBatch(data_matrix)
index.createIndex({'post': 2}, print_progress=True)

# query for the nearest neighbours of the first datapoint
ids, distances = index.knnQuery((-0.057782,-0.404332,0.341625,0.104915,0.037961,-0.058649,-0.008876,-0.302638,-0.287248,0.311828,0.326037,0.074690,-0.273633,-0.288274,-0.111039,0.537903,0.160918,-0.084170,0.495152,0.211059,-0.163284,0.321342,0.454457,-0.303044,0.139545,-0.220430,0.149117,0.116723,0.188942,0.121979,-0.231453,-0.127567,-0.290115,0.067263,-0.071149,-0.065337,0.008104,-0.176944,-0.126016,-0.327164,-0.187286,0.053995,-0.157628,-0.044691,0.072945,0.250530,-0.105324,-0.028852,-0.134184,0.117302,-0.275826,0.088744,-0.173094,0.078366,0.066125,-0.164700,0.051949,-0.122424,-0.062782,-0.333823,0.032330,-0.215043,0.127369,-0.100092,0.113779,0.307699,0.250874,0.107771,0.513093,0.378336,-0.231016,0.002750,0.091664,-0.153093,-0.128322,-0.227670,-0.284374,-0.028690,-0.098221,-0.461779,0.166557,0.310072,-0.119091,-0.404434,0.424108,0.472024,0.254146,0.106314,0.373904,-0.140226,-0.298379,0.340491,-0.169073,0.306000,0.339712,0.113607,-0.072253,0.207012,-0.163373,-0.052949), k=10)



for id, distance in zip(ids, distances):
    print(id, distance)

# get all nearest neighbours for all the datapoint
# using a pool of 4 threads to compute
#neighbours = index.knnQueryBatch(data, k=10, num_threads=4)


