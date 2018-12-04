from pyspark import SparkContext

def validate(record):
    try:
        data = record.strip().split(",")
        if len(data) == 17:
            sample_id = data[0].strip()
            if sample_id.lower() != "sample":
                fsc, ssc = int(data[1]), int(data [2])
                if 1<=fsc<=150000 and 1<=ssc<=150000:
                    return (sample_id, 1)
        return ()
    except:
        return ()

def extract_researcher(record):
    try:
        data = record.strip().split(",")
        if len(data) == 8:
            sample_id = data[0].strip()
            if sample_id.lower() != "sample":
                researchers = data[7].strip()
                if len(researchers) != 0:
                    return [(sample_id, researcher.strip()) for researcher in researchers.split(";")]
        return []
    except:
        return []

def sum_sample_count(reduced_count, current_count):
    return reduced_count+current_count

def map_to_pair(record):
    researcher, count = record
    return (researcher, count)

def map_to_result(record):
    researcher, count = record
    return researcher+"\t"+str(count)

if __name__ == "__main__":
    sc = SparkContext(appName="number of measurements per researcher")
    experiments = sc.textFile("experiments.csv")
    measurements = sc.textFile("measurements")

    sample_count_valid = measurements.map(validate).filter(lambda rec : rec!=()).reduceByKey(sum_sample_count)
    sample_researcher = experiments.flatMap(extract_researcher)
     
    researcher_count = sample_researcher.join(sample_count_valid).values().map(map_to_pair).reduceByKey(sum_sample_count,1)
    
    researcher_count_sorted = researcher_count.sortBy(lambda rec : rec[0], ascending=True).sortBy(lambda rec : rec[1], ascending=False).map(map_to_result)

    researcher_count_sorted.saveAsTextFile("researcher_count_sorted")
