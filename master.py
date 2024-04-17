import random
import grpc
import sys
import mapReduce_pb2
import mapReduce_pb2_grpc
import logging
from mapReduce_pb2 import reduceData,mapData, Point
from mapReduce_pb2_grpc import MasterAndReducerStub
points = []
centroids = []
mapper_ports = {i: 50052 + i - 1 for i in range(1, 6)}
reducer_ports = {i: 50061 + i - 1 for i in range(1, 6)}
threads = []
def callMapper(start_index, end_index, iteration_num, mapper_id, reducers):
    mapper_addr = "localhost:" + str(mapper_ports[mapper_id])
    centroid_points = []
    for i in range(len(centroids)):
        centroid_points.append(mapReduce_pb2.Point(x=centroids[i][0], y=centroids[i][1]))
    map_data = mapReduce_pb2.mapData(startIdx=start_index, endIdx=end_index, iter_num=iteration_num, reducers=reducers,
                                     mapper_id = mapper_id, centroids=centroid_points)
    with grpc.insecure_channel(mapper_addr) as channel:
        try:
            stub = mapReduce_pb2_grpc.MasterAndMapperStub(channel)
            response = stub.mapRequest(map_data)
            if response.state == "SUCCESS":
                return True
            else:
                return False
        except Exception as e:
            return False

def callReducer(iteration_num, reducer_id, work_reducer, mappers):
    reducer_address = f"localhost:{reducer_ports[reducer_id]}"
    request_data = reduceData(iter_num=iteration_num, my_reducer_id=reducer_id,work_reducer_id=work_reducer, mappers=mappers)
    # Initialize connection and stub
    try:
        with grpc.insecure_channel(reducer_address) as channel:
            stub = MasterAndReducerStub(channel)
            response = stub.reduceRequest(request_data)
            if response.state != "SUCCESS":
                return [False, False]
    except grpc.RpcError as rpc_error:
        logging.error(f"gRPC communication error with reducer {reducer_id}: {rpc_error}")
        return [False, False]
    except Exception as e:
        logging.error(f"Unexpected error in callReducer with reducer {reducer_id}: {e}")
        return [False, False]
    # Check for changes in the centroids
    run_again = False
    for pair in response.pairs:
        index = int(pair.centroid_id) - 1
        old_x, old_y = centroids[index]
        new_x, new_y = pair.point.x, pair.point.y
        if abs(old_x - new_x) > 0.001 or abs(old_y - new_y) > 0.001:
            run_again = True
            centroids[index] = (new_x, new_y)  # Update the centroid coordinates
            logging.info(f"Centroid {index + 1} updated: ({new_x:.4f}, {new_y:.4f})")
    return [run_again, True]
def writer(file, mode, data):
    with open(file, mode) as f:
        f.write(data)

if __name__ == '__main__':
    mappers = int(sys.argv[1])
    reducers = int(sys.argv[2])
    num_of_centroids = int(sys.argv[3])
    max_iterations = int(sys.argv[4])
    with open('Input/points.txt') as f:
        points = [(float(x), float(y)) for x, y in (line.split(',') for line in f)]
    # centroids.append(random.choice(points))
    centroids = [points[i] for i in range(num_of_centroids)]
    print("Initial Centroids are: ", centroids)
    writer('dump_master.txt', 'w', f"{centroids}")
    num_of_points = len(points) // mappers
    run_again = True
    for iter in range(1, max_iterations + 1):
        if run_again:
            print("Iteration %d" % (iter))
            start_index = 0
            end_index = num_of_points
            for i in range(1, mappers + 1):
                if i == mappers:
                    end_index = len(points) - 1
                isDone = False
                assigned_mapper = i
                while not isDone:
                    print("Task is assigned to Mapper %d, notifying it via gRPC" % (assigned_mapper))
                    isDone = callMapper(start_index, end_index, iter, assigned_mapper, reducers)
                    if not isDone:
                        print("Mapper %d failed to complete the task assigned to it" % (assigned_mapper))
                        assigned_mapper = (assigned_mapper % mappers) + 1
                    else:
                        print("Mapper %d completed the task assigned to it" % (assigned_mapper))
                start_index = end_index + 1
                end_index = start_index + num_of_points
            run_again = False
            for i in range(1, reducers + 1):
                isDone = False
                assigned_reducer = i
                while not isDone:
                    print(f"Task is assigned to Reducer {assigned_reducer}, notifying it via gRPC")
                    temp = callReducer(iter, assigned_reducer, i, mappers)
                    isDone = temp[1]
                    if not isDone:
                        print(f"Reducer {assigned_reducer} failed to complete the task assigned to it")
                        assigned_reducer = (assigned_reducer % reducers) + 1
                    else:
                        print("Reducer %d completed the task assigned to it" % (assigned_reducer))
                        run_again = run_again or temp[0]

            print(f"Centroids after iteration {iter}: {centroids}")
            writer('dump_master.txt', 'a', f"Centroids after iteration {iter}: {centroids}")
        else:
            print(f"Algorithm converged after {iter} iterations")
            break
    print(f"Final Centroids are: {centroids}")
    writer('dump_master.txt', 'a', f"Final Centroids are: {centroids}")
    with open('centroids.txt', 'a') as f:
        f.writelines([f"{centroid[0]:.4f},{centroid[1]:.4f}\n" for centroid in centroids])