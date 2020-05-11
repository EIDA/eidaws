import os

from setuptools import setup, find_packages


def get_version(filename):
    from re import findall

    with open(filename) as f:
        metadata = dict(findall("__([a-z]+)__ = '([^']+)'", f.read()))
    return metadata["version"]


_AUTHOR = "Daniel Armbruster"
_AUTHOR_EMAIL = "daniel.armbruster@sed.ethz.ch"
_DESCRIPTION = "Endpoint proxy webservice for EIDA"
_VERSION = get_version(os.path.join("eidaws", "endpoint_proxy", "version.py"))
_INCLUDES = "*"
_DEPS = [
    "aiohttp>=3.6.2",
    "aiodns>=2.0.0",
    "brotlipy>=0.7.0",
    "cchardet>=2.1.5",
    "eidaws.utils==0.1",
    "jsonschema>=3.2.0",
    "pyyaml>=5.3",
]
_ENTRY_POINTS = {
    "console_scripts": ["eida-endpoint-proxy = eidaws.endpoint_proxy.app:main"]
}

setup(
    name="eidaws.endpoint_proxy",
    version=_VERSION,
    author=_AUTHOR,
    author_email=_AUTHOR_EMAIL,
    description=_DESCRIPTION,
    long_description=open("README.rst").read(),
    license="GPLv3",
    keywords="seismology waveforms proxy federation eida service",
    url="https://github.com/damb/eidaws/eidaws.endpoint_proxy",
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
    # setup_requires=["pytest-runner",],
    # tests_require=["pytest", "pytest-asyncio", "pytest-aiohttp"],
    python_requires="~=3.7",
)
