'''
Created on May 22, 2017

@author: yang
'''
from pyspark import SparkContext, SparkConf
import random
import sys

# generate initial centroids
def extract_centroids(record):
    data = record.strip().split("\t")
    cluster_id, num_measurements = int(data[0]), int(data[1])
    centroids = [float(coordinate) for coordinate in data[2:]]
    return (cluster_id, centroids)

# extract coordinates from record
def extract_coordinates(record, coordinate_index):
    try:
        data = record.strip().split(",")
        if len(data) == 17:
            sample_id = data[0].strip()
            if sample_id.lower() != "sample":
                fsc, ssc = int(data[1]), int(data [2])
                if 1<=fsc<=150000 and 1<=ssc<=150000:
                    coordinates = [float(data[3+i]) for i in coordinate_index]
                    return coordinates
        return []
    except:
        return []

# assign cluster to record based on distance
def assign_cluster(record, clusters):
        coordinates = record
        min_distance = float("inf")
        cluster_id = -1
        for i in range(len(clusters)):
            distance = 0.0
            for j in range(len(clusters[i])):
                distance += (coordinates[j]-clusters[i][j])**2
                if distance < min_distance:
                    min_distance = distance
                    cluster_id = i
        return (cluster_id, coordinates)
    
# reassign cluster to get distance info
def reassign_cluster(record, clusters):
    coordinates = record
    min_distance = float("inf")
    cluster_id = -1
    for i in range(len(clusters)):
        distance = 0.0
        for j in range(len(clusters[i])):
            distance += (coordinates[j]-clusters[i][j])**2
            if distance < min_distance:
                min_distance = distance
                cluster_id = i
    return (cluster_id, min_distance, coordinates)

# update centroids after clustering    
def update_centroids(record, num_coordinates):
        cluster_id, coordinates_list = record
        coordinates_sum = [0.0 for m in range(num_coordinates)]
        coordinates_avg = [0.0 for n in range(num_coordinates)]
        for coordinates in coordinates_list:
            for i in range(num_coordinates):
                coordinates_sum[i] += coordinates[i]
        num_measurements = len(coordinates_list)
        for j in range(num_coordinates):
            coordinates_avg[j] = round(coordinates_sum[j] / num_measurements, 6)
        return (cluster_id, (num_measurements, coordinates_avg))

# sequence function used in aggregateByKey
def seq(accumulated, current):
    accumulated.append(current)
    return accumulated

# combine function used in aggregateByKey
def comb(accumulated_1, accumulated_2):
    accumulated_1 += accumulated_2
    return accumulated_1

# format record for reclustering
def map_to_recluster(record):
    cluster_id, min_distance, coordinates = record
    return coordinates

# convert record into final result format
def map_to_result(record):
    cluster_id, measurements_centroids = record
    measurements, centroids = measurements_centroids
    result = str(cluster_id) + "\t" + str(measurements)
    for i in range(len(centroids)):
        result += '\t' + str(centroids[i])
    return result



# main function
if __name__ == "__main__":
    sc = SparkContext(appName="reclustering")
    measurements = sc.textFile("measurements/measurements_arcsin200_p1.csv")
    results = sc.textFile("clusterId_num_centroids")
    
# default parameters
    k = 5
    n_iter = 10
    coordinate_names = ['ly6c', 'cd11b', 'sca1']
    coordinates_list = ['cd48', 'ly6g', 'cd117', 'sca1', 'cd11b', 'cd150', 'cd11c', 'b220', 'ly6c', 'cd115', 'cd135', 'cd3cd19nk11', 'cd16cd32', 'cd45']
    coordinate_index = []
    
# customized parameters
    if len(sys.argv) == 2:
        k = int(sys.argv[1])
     
    if len(sys.argv) == 3:
        k = int(sys.argv[1])
        n_iter = int(sys.argv[2])
        
    if len(sys.argv) > 3:
        k = int(sys.argv[1])
        n_iter = int(sys.argv[2])
        coordinate_names = sys.argv[3:]

# get coordinate_index
    for coordinate_name in coordinate_names:
        if coordinate_name not in coordinates_list:
            print('invalid coordinate name')
            exit(1)
        coordinate_index.append(coordinates_list.index(coordinate_name))

# get number of coordinates
    num_coordinates = len(coordinate_names)
    
# extract coordinates from original dataset
    coordinates = measurements.map(lambda rec : extract_coordinates(rec, coordinate_index)).filter(lambda rec : rec!=[])

# get cluster centroids from task2 result
    clusterId_centroids_dict = results.map(extract_centroids).collectAsMap()
    k = len(clusterId_centroids_dict.keys())
    clusters = [0 for i in range(k)]
    for key in clusterId_centroids_dict.keys():
        clusters[key] = clusterId_centroids_dict.get(key)

# reassign cluster to get distance info
    clusterId_distance_cooridinates = coordinates.map(lambda rec : reassign_cluster(rec, clusters)).sortBy(lambda rec : (rec[0],rec[1]), ascending=False, numPartitions=k)
        
# remove outliers
    for idx in range(k):
        distance_per_cluster = clusterId_distance_cooridinates.filter(lambda x : x[0]==idx).map(lambda rec : rec[1]).collect()
        num_measurements = len(distance_per_cluster)
        if num_measurements != 0:
            benchmark = distance_per_cluster[int(num_measurements*0.1)]
            clusterId_distance_cooridinates = clusterId_distance_cooridinates.filter(lambda x : ((x[0]==idx and x[1]<benchmark) or x[0]!=idx))
    coordinates_filtered = clusterId_distance_cooridinates.map(map_to_recluster)


# n iterations of reclustering
    for j in range(n_iter):
        clusterId_coordinatesList = coordinates_filtered.map(lambda rec : assign_cluster(rec, clusters)).aggregateByKey([], seq, comb, 1)
        clusterId_num_centroids = clusterId_coordinatesList.map(lambda rec : update_centroids(rec, num_coordinates))
        clusterId_num_centroids_dict = clusterId_num_centroids.collectAsMap()
        for idx in range(k):
            if idx in clusterId_num_centroids_dict.keys():
                clusters[idx] = clusterId_num_centroids_dict[idx][1]

# add hidden clusters and save final result
    for index in range(k):
            if index not in clusterId_num_centroids_dict.keys():
                clusterId_num_centroids = clusterId_num_centroids.union(sc.parallelize(((index, (0, clusters[index])),)))           
    clusterId_numOfMeasurements_centroids = clusterId_num_centroids.sortBy(lambda rec : rec[0], ascending=True, numPartitions=1).map(map_to_result)
    clusterId_numOfMeasurements_centroids.saveAsTextFile("clusterId_num_centroids_filtered")
    
    
    
    
    
    
    