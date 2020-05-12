import os

from setuptools import setup, find_packages


def get_version(filename):
    from re import findall

    with open(filename) as f:
        metadata = dict(findall("__([a-z]+)__ = '([^']+)'", f.read()))
    return metadata["version"]


_AUTHOR = "Daniel Armbruster"
_AUTHOR_EMAIL = "daniel.armbruster@sed.ethz.ch"
_DESCRIPTION = "Alternative routing webservice for EIDA"
_VERSION = get_version(os.path.join("eidaws", "stationlite", "version.py"))
_INCLUDES = "*"
_DEPS = [
    "cached-property>=1.5.1",
    "eidaws.utils==0.1",
    "fasteners>=0.14.1",
    "Flask>=0.12.2",
    "Flask-RESTful>=0.3.6",
    "Flask-SQLAlchemy>=2.3.2",
    "jsonschema>=3.2.0",
    "lxml>=4.2.0",
    "obspy>=1.2.1",
    "pyyaml>=5.3",
    "requests>=2.18.4",
    "SQLAlchemy>=1.2.0",
]
_EXTRAS = {"postgres": ["psycopg2"]}
_ENTRY_POINTS = {
    "console_scripts": [
        "eida-stationlite-db-init = eidaws.stationlite.harvest.misc:db_init",
        "eida-stationlite-harvest = eidaws.stationlite.harvest.app:main",
    ]
}

setup(
    name="eidaws.stationlite",
    version=_VERSION,
    author=_AUTHOR,
    author_email=_AUTHOR_EMAIL,
    description=_DESCRIPTION,
    long_description=open("README.rst").read(),
    license="GPLv3",
    keywords="seismology waveforms federation routing eida service",
    url="https://github.com/damb/eidaws/eidaws.stationlite",
    platforms=["Linux",],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
        "Topic :: Scientific/Engineering",
    ],
    packages=find_packages(include=_INCLUDES),
    zip_safe=False,
    entry_points=_ENTRY_POINTS,
    install_requires=_DEPS,
    extras_require=_EXTRAS,
    setup_requires=["pytest-runner",],
    tests_require=["pytest",],
    python_requires="~=3.7",
)
