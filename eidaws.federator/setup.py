import os

from setuptools import setup


def get_version(filename):
    from re import findall

    with open(filename) as f:
        metadata = dict(findall("__([a-z]+)__ = '([^']+)'", f.read()))
    return metadata["version"]


_AUTHOR = "Daniel Armbruster"
_AUTHOR_EMAIL = "daniel.armbruster@sed.ethz.ch"
_DESCRIPTION = "Federating webservice for EIDA"
_VERSION = get_version(os.path.join("eidaws", "federator", "version.py"))
_DEPS = [
    "aiohttp>=3.6.2",
    "aiohttp_cors>=0.7.0",
    "aiohttp-remotes>=0.1.2",
    "aiodns>=2.0.0",
    "aiofiles>=0.5.0",
    "aioredis>=1.3.1",
    "brotlipy>=0.7.0",
    "cached-property>=1.5.1",
    "cchardet>=2.1.5",
    "eidaws.utils==0.1",
    "jsonschema>=3.2.0",
    "lxml>=4.5.0",
    "pyyaml>=5.3",
    "yarl==1.5.1",
]
_ENTRY_POINTS = {
    "console_scripts": [
        (
            "eida-federator-wfcatalog-json="
            "eidaws.federator.eidaws_wfcatalog.json.app:main"
        ),
        (
            "eida-federator-availability-geocsv="
            "eidaws.federator.fdsnws_availability.geocsv.app:main"
        ),
        (
            "eida-federator-availability-json="
            "eidaws.federator.fdsnws_availability.json.app:main"
        ),
        (
            "eida-federator-availability-request="
            "eidaws.federator.fdsnws_availability.request.app:main"
        ),
        (
            "eida-federator-availability-text="
            "eidaws.federator.fdsnws_availability.text.app:main"
        ),
        (
            "eida-federator-dataselect-miniseed="
            "eidaws.federator.fdsnws_dataselect.miniseed.app:main"
        ),
        (
            "eida-federator-station-text="
            "eidaws.federator.fdsnws_station.text.app:main"
        ),
        (
            "eida-federator-station-xml="
            "eidaws.federator.fdsnws_station.xml.app:main"
        ),
    ]
}

setup(
    name="eidaws_federator",
    version=_VERSION,
    author=_AUTHOR,
    author_email=_AUTHOR_EMAIL,
    description=_DESCRIPTION,
    long_description=open("README.rst").read(),
    license="GPLv3",
    keywords="seismology waveforms federation eida service",
    url="https://github.com/damb/eidaws/eidaws.federator",
    platforms=["Linux"],
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
    packages=["eidaws.federator"],
    zip_safe=False,
    entry_points=_ENTRY_POINTS,
    install_requires=_DEPS,
    setup_requires=["pytest-runner"],
    tests_require=["pytest", "pytest-asyncio", "pytest-aiohttp"],
    python_requires="~=3.7",
)
