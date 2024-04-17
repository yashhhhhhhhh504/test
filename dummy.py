import grpc
import mapReduce_pb2
import mapReduce_pb2_grpc
import logging
import sys
import os
from concurrent import futures
def writer(file, mode, data):
    with open(file, mode) as f:
        f.write(data)
def partition(pairs, reducers, mapper_id, iteration):
    i = 1
    while i <= reducers:
        if os.path.exists(f"Mappers/M{mapper_id}d/R{i}.txt"):
            with open(f"Mappers/M{mapper_id}/R{i}.txt", 'r') as file:
                it = file.readline()
                if int(it) != iteration:
                    writer(f"Mappers/M{mapper_id}/R{i}.txt", "w", f"{iteration}\n")
        else:
            writer(f"Mappers/M{mapper_id}/R{i}.txt", "w", f"{iteration}\n")
        i += 1

    for key in pairs:
        file_num = key % reducers
        message = '\n'.join([f"{key}.4f,{pair.x}.4f,{pair.y}.4f" for pair in pairs[key]])
        writer(f"Mappers/M{mapper_id}/R{file_num + 1}.txt", 'a', message)


def distance(point1, point2):
    return ((point1.x - point2.x) ** 2 + (point1.y - point2.y) ** 2) ** 0.5


class MasterAndMapper(mapReduce_pb2_grpc.MasterAndMapperServicer):
    def mapRequest(self, request, context):
        startIdx = request.startIdx
        endIdx = request.endIdx
        iter_num = request.iter_num
        centroids = request.centroids
        reducers = request.reducers
        mapper_id = request.mapper_id

        print("Mapper received request for iteration %d" % iter_num)
        print("Start index: %d, End index: %d" % (startIdx, endIdx))
        writer(f'Mappers/M{mapper_id}d/dump.txt', 'a', f"Mapper received request for iteration {iter_num}\n")
        writer(f'Mappers/M{mapper_id}d/dump.txt', 'a', f"Start index: {startIdx}, End index: {endIdx}\n")
            
        with open('Input/points.txt') as f:
            points = [(float(x), float(y)) for x, y in (line.split(',') for line in f)]

        try:
            pairs = {}

            for i in range(startIdx, endIdx + 1):
                min_dist = sys.maxsize
                min_idx = -1
                for j in range(1, len(centroids) + 1):
                    dist = distance(mapReduce_pb2.Point(x=points[i][0], y=points[i][1]), centroids[j - 1])
                    if dist < min_dist:
                        min_dist = dist
                        min_idx = j
                if min_idx in pairs:
                    pairs[min_idx].append(mapReduce_pb2.Point(x=points[i][0], y=points[i][1]))
                else:
                    pairs[min_idx] = [mapReduce_pb2.Point(x=points[i][0], y=points[i][1])]

            partition(pairs, reducers, mapper_id, iter_num)

            print("Writing pairs to dump.txt")
            writer(f'Mappers/M{mapper_id}d/dump.txt', 'a', pairs.__str__())
            return mapReduce_pb2.status(state="SUCCESS")
        
        except Exception as e:
            return mapReduce_pb2.status(state="FAILURE")


class MapperAndReducer(mapReduce_pb2_grpc.MapperAndReducerServicer):
    def getPairs(self, request, context):
        reducer_id = request.reducer_id
        mapper_id = request.mapper_id

        # read from file Rn.txt
        file = open("Mappers/M%d/R%d.txt" % (mapper_id, reducer_id), "r")
        pairs = []
        skippedFirst = False
        for line in file:
            if not skippedFirst:
                skippedFirst = True
                continue
            centroid_id, x, y = line.split(',')
            pairs.append(
                mapReduce_pb2.Pair(centroid_id=float(centroid_id), point=mapReduce_pb2.Point(x=float(x), y=float(y))))
        file.close()

        return mapReduce_pb2.Pairs(pairs=pairs)


if __name__ == "__main__":
    logging.basicConfig()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapReduce_pb2_grpc.add_MasterAndMapperServicer_to_server(MasterAndMapper(), server)
    mapReduce_pb2_grpc.add_MapperAndReducerServicer_to_server(MapperAndReducer(), server)
    server.add_insecure_port("[::]:50053")
    server.start()
    print("Mapper server started, listening on 50053")
    server.wait_for_termination()