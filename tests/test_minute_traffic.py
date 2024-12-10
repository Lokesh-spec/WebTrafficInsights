import json
import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipelines.minute_traffic import parse_json, add_timestamp, EventLog, AddWindowTS  # Ensure these functions are implemented

@pytest.fixture
def test_gcs_bucket_input():
    input_data = [
        '{"ip": "17.84.224.106", "id": "-1856121532364392091", "lat": 37.323, "lng": -122.0322, "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0", "age_bracket": "55+", "opted_into_marketing": false, "http_request": "GET protozoa.html HTTP/1.0", "http_response": 200, "file_size_bytes": 219, "event_datetime": "2024-12-10T08:09:36.925+00:00", "event_ts": 1733818176925}',
        '{"ip": "54.72.240.0", "id": "-7228083391147584758", "lat": 53.3331, "lng": -6.2489, "user_agent": "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00", "age_bracket": "26-40", "opted_into_marketing": true, "http_request": "GET bacteria.html HTTP/1.0", "http_response": 200, "file_size_bytes": 416, "event_datetime": "2024-12-10T08:09:23.050+00:00", "event_ts": 1733818163050}',
        '{"ip": "54.72.240.0", "id": "-7228083391147584758", "lat": 53.3331, "lng": -6.2489, "user_agent": "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00", "age_bracket": "26-40", "opted_into_marketing": true, "http_request": "GET fungi.html HTTP/1.0", "http_response": 200, "file_size_bytes": 200, "event_datetime": "2024-12-10T08:08:46.919+00:00", "event_ts": 1733818126919}',
        '{"ip": "17.84.224.106", "id": "-1856121532364392091", "lat": 37.323, "lng": -122.0322, "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0", "age_bracket": "55+", "opted_into_marketing": false, "http_request": "GET archea.html HTTP/1.0", "http_response": 200, "file_size_bytes": 298, "event_datetime": "2024-12-10T08:08:48.907+00:00", "event_ts": 1733818128907}',
        '{"ip": "17.84.224.106", "id": "-1856121532364392091", "lat": 37.323, "lng": -122.0322, "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0", "age_bracket": "55+", "opted_into_marketing": false, "http_request": "GET fungi.html HTTP/1.0", "http_response": 200, "file_size_bytes": 322, "event_datetime": "2024-12-10T08:09:29.041+00:00", "event_ts": 1733818169041}',
        '{"ip": "54.72.240.0", "id": "-7228083391147584758", "lat": 53.3331, "lng": -6.2489, "user_agent": "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00", "age_bracket": "26-40", "opted_into_marketing": true, "http_request": "GET archea.html HTTP/1.0", "http_response": 200, "file_size_bytes": 404, "event_datetime": "2024-12-10T08:09:16.076+00:00", "event_ts": 1733818156076}',
        '{"ip": "54.72.240.0", "id": "-7228083391147584758", "lat": 53.3331, "lng": -6.2489, "user_agent": "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00", "age_bracket": "26-40", "opted_into_marketing": true, "http_request": "GET coniferophyta.html HTTP/1.0", "http_response": 200, "file_size_bytes": 430, "event_datetime": "2024-12-10T08:08:44.617+00:00", "event_ts": 1733818124617}',
        '{"ip": "17.84.224.106", "id": "-1856121532364392091", "lat": 37.323, "lng": -122.0322, "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0", "age_bracket": "55+", "opted_into_marketing": false, "http_request": "GET blastocladiomycota.html HTTP/1.0", "http_response": 200, "file_size_bytes": 360, "event_datetime": "2024-12-10T08:09:10.923+00:00", "event_ts": 1733818150923}',
        '{"ip": "54.72.240.0", "id": "-7228083391147584758", "lat": 53.3331, "lng": -6.2489, "user_agent": "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00", "age_bracket": "26-40", "opted_into_marketing": true, "http_request": "GET animalia.html HTTP/1.0", "http_response": 200, "file_size_bytes": 355, "event_datetime": "2024-12-10T08:10:11.076+00:00", "event_ts": 1733818211076}',
        '{"ip": "17.84.224.106", "id": "-1856121532364392091", "lat": 37.323, "lng": -122.0322, "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0", "age_bracket": "55+", "opted_into_marketing": false, "http_request": "GET archaea.html HTTP/1.0", "http_response": 200, "file_size_bytes": 417, "event_datetime": "2024-12-10T08:09:12.100+00:00", "event_ts": 1733818152100}'
    ]
    
    # expected_output = [  
    #     EventLog(
    #     "17.84.224.106", "-1856121532364392091", 37.323, -122.0322,
    #     "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0",
    #     "55+", False, "GET protozoa.html HTTP/1.0", 200, 219,
    #     "2024-12-10T08:09:36.925+00:00", 1733818176925
    #     ),
    #     EventLog(
    #         "54.72.240.0", "-7228083391147584758", 53.3331, -6.2489,
    #         "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00",
    #         "26-40", True, "GET bacteria.html HTTP/1.0", 200, 416,
    #         "2024-12-10T08:09:23.050+00:00", 1733818163050
    #     ),
    #     EventLog(
    #         "54.72.240.0", "-7228083391147584758", 53.3331, -6.2489,
    #         "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00",
    #         "26-40", True, "GET fungi.html HTTP/1.0", 200, 200,
    #         "2024-12-10T08:08:46.919+00:00", 1733818126919
    #     ),
    #     EventLog(
    #         "17.84.224.106", "-1856121532364392091", 37.323, -122.0322,
    #         "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0",
    #         "55+", False, "GET archea.html HTTP/1.0", 200, 298,
    #         "2024-12-10T08:08:48.907+00:00", 1733818128907
    #     ),
    #     EventLog(
    #         "17.84.224.106", "-1856121532364392091", 37.323, -122.0322,
    #         "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0",
    #         "55+", False, "GET fungi.html HTTP/1.0", 200, 322,
    #         "2024-12-10T08:09:29.041+00:00", 1733818169041
    #     ),
    #     EventLog(
    #         "54.72.240.0", "-7228083391147584758", 53.3331, -6.2489,
    #         "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00",
    #         "26-40", True, "GET archea.html HTTP/1.0", 200, 404,
    #         "2024-12-10T08:09:16.076+00:00", 1733818156076
    #     ),
    #     EventLog(
    #         "54.72.240.0", "-7228083391147584758", 53.3331, -6.2489,
    #         "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00",
    #         "26-40", True, "GET coniferophyta.html HTTP/1.0", 200, 430,
    #         "2024-12-10T08:08:44.617+00:00", 1733818124617
    #     ),
    #     EventLog(
    #         "17.84.224.106", "-1856121532364392091", 37.323, -122.0322,
    #         "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0",
    #         "55+", False, "GET blastocladiomycota.html HTTP/1.0", 200, 360,
    #         "2024-12-10T08:09:10.923+00:00", 1733818150923
    #     ),
    #     EventLog(
    #         "54.72.240.0", "-7228083391147584758", 53.3331, -6.2489,
    #         "Opera/8.30.(X11; Linux x86_64; ln-CD) Presto/2.9.180 Version/12.00",
    #         "26-40", True, "GET animalia.html HTTP/1.0", 200, 355,
    #         "2024-12-10T08:10:11.076+00:00", 1733818211076
    #     ),
    #     EventLog(
    #         "17.84.224.106", "-1856121532364392091", 37.323, -122.0322,
    #         "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0",
    #         "55+", False, "GET archaea.html HTTP/1.0", 200, 417,
    #         "2024-12-10T08:09:12.100+00:00", 1733818152100
    #     )
    # ]
    
    expected_output = [
        {
            "window_start": "2024-12-10T08:08:00",
            "window_end": "2024-12-10T08:09:00",
            "page_views": 3
        },
        {
            "window_start": "2024-12-10T08:09:00",
            "window_end": "2024-12-10T08:10:00",
            "page_views": 6
        },
        {
            "window_start": "2024-12-10T08:10:00",
            "window_end": "2024-12-10T08:11:00",
            "page_views": 1
        }
    ]
    
    return input_data, expected_output

def test_parse_json(test_gcs_bucket_input):
    """Test the parse_json function."""
    input_data, _ = test_gcs_bucket_input  

    parsed = [parse_json(record) for record in input_data]
    
    assert len(parsed) == 10 
    
    assert parsed[0].ip == "17.84.224.106"
    assert parsed[0].lat == 37.323
    assert parsed[0].lng == -122.0322
    assert parsed[0].user_agent == "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_6 like Mac OS X) AppleWebKit/531.0 (KHTML, like Gecko) CriOS/13.0.895.0 Mobile/67S405 Safari/531.0"
    assert parsed[0].age_bracket == "55+"
    assert parsed[0].opted_into_marketing is False
    assert parsed[0].http_request == "GET protozoa.html HTTP/1.0"
    assert parsed[0].http_response == 200
    assert parsed[0].file_size_bytes == 219
    assert parsed[0].event_datetime == "2024-12-10T08:09:36.925+00:00"
    assert parsed[0].event_ts == 1733818176925

def test_pipeline(test_gcs_bucket_input):
    input_data, expected_output = test_gcs_bucket_input

    with TestPipeline() as p:
        input_data_pcol = p | beam.Create(input_data)

        processed_data = (
            input_data_pcol
            | "ParseJson" >> beam.Map(parse_json)
            | "AddTimeStamp" >> beam.Map(add_timestamp)
        )
        
        windowed_data = (
            processed_data
            | beam.WindowInto(beam.window.FixedWindows(60))  # Adjust the window size as needed
            | beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
            | beam.ParDo(AddWindowTS())
        )

        assert_that(windowed_data, equal_to(expected_output))  # Check for expected output
