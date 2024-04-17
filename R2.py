import grpc
import mapReduce_pb2
import mapReduce_pb2_grpc
import logging
import json
from concurrent import futures
from collections import defaultdict


def get_pairs_from_mapper(mapper_addr, work_reducer_id, mapper):
    with grpc.insecure_channel(mapper_addr) as channel:
        try:
            stub = mapReduce_pb2_grpc.MapperAndReducerStub(channel)
            response = stub.getPairs(mapReduce_pb2.Identity(reducer_id=work_reducer_id, mapper_id=mapper))
            return [(pair.centroid_id, pair.point.x, pair.point.y) for pair in response.pairs]
        except grpc.RpcError as e:
            print(f"Error in connecting to mapper at {mapper_addr}")
            return []


def shuffle_and_sort(reducer_id, mappers, work_reducer_id):
    print(f"Reducer ID {reducer_id} is shuffling and sorting for reducer {work_reducer_id}")
    pairs = {}
    port = 50052
    for mapper in range(1, mappers + 1):
        mapper_addr = f"localhost:{port}"
        mapper_pairs = get_pairs_from_mapper(mapper_addr, work_reducer_id, mapper)
        for centroid_id, x, y in mapper_pairs:
            pairs.setdefault(centroid_id, []).append((x, y))
        port += 1
    return pairs


def calculate_new_centroids(pairs):
    new_centroids = defaultdict(lambda: [0, 0])
    for key, point_list in pairs.items():
        x_sum = sum(point[0] for point in point_list)
        y_sum = sum(point[1] for point in point_list)
        new_centroids[key][0] += x_sum
        new_centroids[key][1] += y_sum

    for key, (x_sum, y_sum) in new_centroids.items():
        num_points = len(pairs[key])
        new_centroids[key] = (x_sum / num_points, y_sum / num_points)

    return new_centroids

def write_new_centroids_to_file(new_centroids, reducer_id):
    with open(f'Reducers/R{reducer_id}/dump.txt', 'a') as f:
        f.write("New centroids:\n")
        json.dump(new_centroids, f)
        f.write("\n")

def prepare_response_pairs(new_centroids):
    ret_pairs = []
    for key, (x, y) in new_centroids.items():
        ret_pairs.append(mapReduce_pb2.Pair(
            centroid_id=float(key),
            point=mapReduce_pb2.Point(x=float(x), y=float(y))
        ))
    return ret_pairs

class MasterAndReducer(mapReduce_pb2_grpc.MasterAndReducerServicer):

    def reduceRequest(self, request, context):
        iter_num = request.iter_num
        my_reducer_id = request.my_reducer_id
        work_reducer_id = request.work_reducer_id
        mappers = request.mappers

        print(f"Reducer received request for iteration {iter_num} of tasks for reducer {work_reducer_id}")

        # Write request information to dump file
        with open(f'Reducers/R{my_reducer_id}/dump.txt', 'a') as f:
            f.write(f"Reducer received request for iteration {iter_num}\n")

        try:
            # Shuffle and sort pairs
            pairs = shuffle_and_sort(my_reducer_id, mappers, work_reducer_id)

            # Calculate new centroids
            new_centroids = calculate_new_centroids(pairs)

            # Write new centroids to dump file
            write_new_centroids_to_file(new_centroids, my_reducer_id)

            # Prepare response pairs
            ret_pairs = prepare_response_pairs(new_centroids)

            print("Returning response")
            return mapReduce_pb2.ReduceResp(pairs=ret_pairs, state="SUCCESS")

        except Exception as e:
            print("Error:", e)
            return mapReduce_pb2.ReduceResp(pairs=[], state="FAILURE")
if __name__ == "__main__":
    logging.basicConfig()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapReduce_pb2_grpc.add_MasterAndReducerServicer_to_server(MasterAndReducer(), server)
    server.add_insecure_port("[::]:50062")
    server.start()
    print("Reducer server started, listening on 50062")
    server.wait_for_termination()