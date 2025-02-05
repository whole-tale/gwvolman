from setuptools import setup, find_packages

setup(
    name="gwvolman",
    version="2.1.0",
    description="WholeTale Girder Volume Manager",
    author="Kacper Kowalik",
    author_email="xarthisius.kk@gmail.com",
    license="BSD-3-Clause",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Science/Research",
        "Natural Language :: English",
        "Programming Language :: Python",
    ],
    entry_points={
        "girder_worker_plugins": [
            "gwvolman = gwvolman:GWVolumeManagerPlugin",
        ]
    },
    install_requires=[
        "girder-client",
        "girder-worker>=5.0.0a5.dev0",
        "kubernetes",
        "docker>=2.3.0",
        "requests",
        "markdown",
        "lxml[html_clean]",
        "pystache",
        "celery[redis]>5",
        "python-dateutil",
    ],
    packages=find_packages(),
    zip_safe=False,
)
