import json
from datetime import datetime
from synchrophasor.pdc import Pdc
from synchrophasor.frame import DataFrame

def timestamp_to_hours(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

def get_stream_name(stream_id):
    stream_names = {
        2001: "CH.PAGUI",
        2002: "EZEIZA",
        2003: "R.OESTE",
        2004: "RDIAMANTE",
        2005: "SGDARG"
    }
    return stream_names.get(stream_id, "Unknown")

if __name__ == "__main__":
    pdc = Pdc(pdc_id=2000, pmu_ip="192.168.160.23", pmu_port=20001)
    pdc.logger.setLevel("DEBUG")

    pdc.run()  # Connect to PMU
    config = pdc.get_config()  # Get configuration from PMU
    pdc.start()  # Request to start sending measurements

    data_array = []
    try:
        while True:
            data = pdc.get()  # Keep receiving data
            if isinstance(data, DataFrame):
                measurements = data.get_measurements()
                for entry in measurements['measurements']:
                    data_row = {
                        'time': timestamp_to_hours(measurements['time']),
                        'stream_id': get_stream_name(entry['stream_id']),
                        'frequency': entry['frequency']
                    }
                    data_array.append(data_row)
            else:
                if not data:
                    pdc.quit()  # Close connection
                    break

                if len(data) > 0:
                    for meas in data:
                        measurements = meas.get_measurements()
                        for measurement in measurements:
                            for entry in measurement['measurements']:
                                data_row = {
                                    'time': timestamp_to_hours(measurement['time']),
                                    'stream_id': get_stream_name(entry['stream_id']),
                                    'frequency': entry['frequency']
                                }
                                data_array.append(data_row)
            
            # Agregar los datos recopilados al archivo JSON
            with open('data.json', 'w') as json_file:
                json.dump(data_array, json_file, indent=4)

    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Stopping data reception.")
