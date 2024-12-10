from setuptools import setup, find_packages

setup(
    name="user_traffic_pipeline",
    version="0.1",
    author="Lokesh K V",
    author_email="kvlokesh18@gmail.com",
    description="Apache Beam pipeline to analyze user traffic data.",
    packages=find_packages(),
    install_requires=[
        "apache-beam[gcp]==2.61.0",  
        "pytest"                     
    ],
    python_requires=">=3.9",          
)
