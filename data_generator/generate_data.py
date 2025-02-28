import os
import json
import uuid
import argparse
import datetime
import time
import math
from faker import Faker
import geocoder
import random

from pprint import pprint
from kafka import KafkaProducer

class EventGenerator:
    def __init__(
        self,
        source : str,
        num_users : int,
        num_events : int,
        max_lag_seconds : int,
        delay_seconds : float,
        bootstrap_servers : str,
        topic_name : str,
        file_name : str = str(uuid.uuid4())
    ):
        self.source = source
        self.num_users = num_users
        self.num_events = num_events
        self.max_lag_seconds = max_lag_seconds
        self.delay_seconds = delay_seconds
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.file_name = file_name
        self.user_pool = self.create_user_pool()
        if self.source == "streaming":
            self.kafka_producer = self.create_producer()
            
    
    def create_user_pool(self):
        init_fields = [
            "ip",
            "id",
            "lat",
            "lng",
            "user_agent",
            "age_bracket",
            "opted_into_marketing"
        ]

        user_pool = [
            dict(zip(init_fields, self.set_initial_values()))
            for _ in range(self.num_users)
        ]
        return user_pool
    
    def set_initial_values(self, faker=Faker()):
        ip = faker.ipv4()
        lookup = geocoder.ip(ip)

        try:
            lat, lng = lookup.latlng
        except Exception:
            lat, lng = "", ""   
        
        id = str(hash(f"{ip}{lat}{lng}"))
        
        user_agents = random.choice(
            [
                faker.firefox,
                faker.chrome,
                faker.safari,
                faker.internet_explorer,
                faker.opera
            ]
        )()
        
        age_bracket = random.choice(["18-25", "26-40", "41-55", "55+"])
        opted_into_marketing = random.choice([True, False])
        
        return ip, id, lat, lng, user_agents, age_bracket, opted_into_marketing
    
    def set_req_info(self):
        """
        Returns a tuple of HTTP request information - http_request, http_response, file_size_bytes
        """
        uri = random.choice(
            [
                "home.html",
                "archea.html",
                "archaea.html",
                "bacteria.html",
                "eucharya.html",
                "protozoa.html",
                "amoebozoa.html",
                "chromista.html",
                "cryptista.html",
                "plantae.html",
                "coniferophyta.html",
                "fungi.html",
                "blastocladiomycota.html",
                "animalia.html",
                "acanthocephala.html",
            ]
        )
        file_size_bytes = random.choice(range(100, 500))
        http_request = f"{random.choice(['GET'])} {uri} HTTP/1.0"
        http_response = random.choice([200])
        return http_request, http_response, file_size_bytes
    
    def append_to_file(self, event: dict):
        """
        Appends a website visit event record into an event output file.
        """
        parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        with open(
            os.path.join(parent_dir, "inputs", f"{self.file_name}.out"), "a"
        ) as fp:
            fp.write(f"{json.dumps(event)}\n")
    
    def generate_events(self):
        num_events = 0
        
        while True:
            num_events += 1
            if num_events > self.num_events:
                break
            
            event_ts = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                seconds=random.uniform(0, self.max_lag_seconds)
            )
            
            req_info = dict(
                zip(
                    ["http_request", "http_response", "file_size_bytes"],
                    self.set_req_info(),
                )
            )
            
            event = {
                **random.choice(self.user_pool),
                **req_info,
                **{
                    "event_datetime" : event_ts.isoformat(timespec="milliseconds"),
                    "event_ts" : int(event_ts.timestamp() * 1000),
                }
            }
            divide_by = 100 if self.source == "batch" else 10
            if num_events % divide_by == 0:
                print(f"{num_events} events created so far...")
                print(event)
            if self.source == "batch":
                self.append_to_file(event)
            else:
                self.send_to_kafka(event)
                time.sleep(self.delay_seconds or 0)
            
        



if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(__file__, description="Web Server Data Generator")
    parser.add_argument(
        "--source",
        "-S",
        type=str,
        default="batch",
        choices=["batch", "streaming"],
        help="The data source - batch or streaming."
    )
    
    parser.add_argument(
        "--num_users",
        "-U",
        type=int,
        default=50,
        help="The number of users to create."
    )
    
    parser.add_argument(
        "--num_events",
        "-E",
        type=int,
        default=math.inf,
        help="The number of events to create."
    )
    
    parser.add_argument(
        "--max_lag_seconds",
        "-L",
        type=int,
        default=0,
        help="The maximum seconds that a record can be lagged."
    )
    
    parser.add_argument(
        "--delay_seconds",
        "-D",
        type=float,
        default=None,
        help="The amount of time  that a record should be delayed. Only applicable for 'Streaming'"
    )
    
    args = parser.parse_args()
    
    source = args.source
    num_users = args.num_users
    num_events = args.num_events
    max_lag_seconds = args.max_lag_seconds
    delay_seconds = args.delay_seconds
    
    gen = EventGenerator(
        source,
        num_users,
        num_events,
        max_lag_seconds,
        delay_seconds,
        os.getenv("BOOTSTRAP_SERVERS", "localhost:29092"),
        os.getenv("TOPIC_NAME", "website-visit"),
    )
    gen.generate_events()