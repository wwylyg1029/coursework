import csv
from multiprocessing import Pool

# MapReduce-like function to count flights per passenger
def mapper(data):
    passenger_id = data[0]
    return (passenger_id, 1)

def reducer(mapped_data):
    flight_count = {}
    for passenger_id, count in mapped_data:
        flight_count[passenger_id] = flight_count.get(passenger_id, 0) + count
    return flight_count

def main():
    # Read passenger data
    passenger_data = []
    with open('AComp_Passenger_data_no_error.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            passenger_data.append(row)

    # Read airport data
    airport_data = {}
    with open('Top30_airports_LatLong.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if len(row) >= 4:
                airport_data[row[1]] = (float(row[2]), float(row[3]))

    # MapReduce-like operation to count flights per passenger
    with Pool() as pool:
        mapped_data = pool.map(mapper, passenger_data)
        reduced_data = reducer(mapped_data)

    # Find passenger with highest number of flights
    max_flights = 0
    max_passenger = ''
    for passenger, count in reduced_data.items():
        if count > max_flights:
            max_flights = count
            max_passenger = passenger

    print(f"Passenger {max_passenger} had the most flights with {max_flights} flights.")

if __name__ == '__main__':
    main()
